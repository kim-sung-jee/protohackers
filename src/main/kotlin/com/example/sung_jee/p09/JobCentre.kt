package com.example.sung_jee.p09

import com.example.sung_jee.Profiles
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets.US_ASCII
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

@Profile(Profiles.P09)
@Configuration
class JobCentreConfig {
    @Bean(initMethod = "start", destroyMethod = "stop")
    fun jobCentre() = JobCentreServer(port = 7007, workerThreads = 1000)
}

private data class Job(
    val id: Long,
    val queue: String,
    val pri: Int,
    val payload: ObjectNode,
) : Comparable<Job> {
    override fun compareTo(other: Job): Int {
        val c = other.pri.compareTo(this.pri)
        return if (c != 0) c else this.id.compareTo(other.id)
    }
}

private class ClientSession(val sock: Socket) {
    val working: LinkedHashSet<Long> = linkedSetOf()

    var waiter: Waiter? = null
}

private class Waiter(
    val client: ClientSession,
    val queues: List<String>,
    val handoff: ArrayBlockingQueue<Job> = ArrayBlockingQueue(1)
)

private class JobCore {
    private val idGen = AtomicLong(1000)
    private val lock = Any()

    private val queues = HashMap<String, PriorityQueue<Job>>()
    private val jobsById = HashMap<Long, Job>()
    private val inFlight = HashMap<Long, ClientSession>()
    private val waiters = mutableListOf<Waiter>()

    fun newId(): Long = idGen.incrementAndGet()

    fun put(job: Job): Pair<Waiter, Job>? {
        synchronized(lock) {
            val pq = queues.computeIfAbsent(job.queue) { PriorityQueue() }
            pq.add(job)
            jobsById[job.id] = job

            return waiters
                .asSequence()
                .mapNotNull { w -> takeBestLocked(w.queues)?.let { j -> w to j } }
                .firstOrNull()
        }
    }

    fun tryGet(ctx: ClientSession, qNames: List<String>): Job? {
        var job: Job? = null
        synchronized(lock) {
            job = takeBestLocked(qNames)
            if (job != null) {
                inFlight[job.id] = ctx
                ctx.working += job.id
            }
        }
        return job
    }

    fun registerWait(ctx: ClientSession, qNames: List<String>): Pair<Job?, Waiter?> {
        synchronized(lock) {
            val j = takeBestLocked(qNames)
            if (j != null) {
                inFlight[j.id] = ctx
                ctx.working += j.id
                return j to null
            }
            val w = Waiter(ctx, qNames)
            ctx.waiter = w
            waiters.add(w)
            return null to w
        }
    }

    fun delete(id: Long): Boolean {
        synchronized(lock) {
            val j = jobsById.remove(id) ?: return false
            inFlight.remove(id)?.working?.remove(id)
            queues[j.queue]?.remove(j)
            return true
        }
    }

    class NotOwner : RuntimeException()

    fun abort(ctx: ClientSession, id: Long): Boolean? {
        synchronized(lock) {
            val j = jobsById[id] ?: return null
            val owner = inFlight[id] ?: return null
            if (owner !== ctx) throw NotOwner()

            inFlight.remove(id)
            ctx.working.remove(id)
            queues.computeIfAbsent(j.queue) { PriorityQueue() }.add(j)
            return true
        }
    }

    fun onDisconnect(ctx: ClientSession): List<Job> {
        val requeued = mutableListOf<Job>()
        synchronized(lock) {
            ctx.waiter?.let { waiters.remove(it) }
            ctx.waiter = null

            for (jid in ctx.working.toList()) {
                val j = jobsById[jid] ?: continue
                inFlight.remove(jid)
                queues.computeIfAbsent(j.queue) { PriorityQueue() }.add(j)
                requeued += j
            }
            ctx.working.clear()
        }
        return requeued
    }

    fun satisfyWaiters() {
        while (true) {
            val picked: Pair<Waiter, Job> = synchronized(lock) {
                var found: Pair<Waiter, Job>? = null
                val it = waiters.iterator()
                while (it.hasNext()) {
                    val w = it.next()
                    val j = takeBestLocked(w.queues)
                    if (j != null) {
                        inFlight[j.id] = w.client
                        w.client.working += j.id
                        it.remove()
                        found = w to j
                        break
                    }
                }
                found
            } ?: break

            val (w, job) = picked
            if (!w.handoff.offer(job)) {
                synchronized(lock) {
                    inFlight.remove(job.id)
                    w.client.working.remove(job.id)
                    queues.computeIfAbsent(job.queue) { PriorityQueue() }.add(job)
                }
            }
        }
    }

    private fun takeBestLocked(qNames: List<String>): Job? {
        var bestQ: PriorityQueue<Job>? = null
        var best: Job? = null
        for (q in qNames) {
            val pq = queues[q] ?: continue
            val top = pq.peek() ?: continue
            if (best == null || top < best) {
                best = top
                bestQ = pq
            }
        }
        if (best != null) bestQ!!.poll()
        return best
    }
}

class JobCentreServer(
    private val port: Int,
    private val workerThreads: Int,
    private val boss: ExecutorService = Executors.newSingleThreadExecutor { r -> Thread(r, "job-boss") },
    private val workers: ExecutorService = Executors.newFixedThreadPool(workerThreads) { r -> Thread(r, "job-wkr") },
) {
    private val running = AtomicBoolean(false)

    @Volatile
    private var serverSocket: ServerSocket? = null

    private val core = JobCore()

    fun start() {
        if (!running.compareAndSet(false, true)) return
        serverSocket = ServerSocket(port)
        boss.execute {
            while (running.get()) {
                val s = serverSocket!!.accept().apply { tcpNoDelay = true }
                workers.execute { handleClient(ClientSession(s)) }
            }
        }
    }

    fun stop() {
        running.set(false)
        runCatching { serverSocket?.close() }
        boss.shutdownNow()
        workers.shutdownNow()
        workers.awaitTermination(2, TimeUnit.SECONDS)
    }

    private fun handleClient(ctx: ClientSession) {
        val sock = ctx.sock
        BufferedReader(InputStreamReader(sock.getInputStream(), US_ASCII)).use { r ->
            BufferedWriter(OutputStreamWriter(sock.getOutputStream(), US_ASCII)).use { w ->
                try {
                    while (true) {
                        val line = r.readLine() ?: break
                        val root: JsonNode = try {
                            mapper.readTree(line)
                        } catch (_: Exception) {
                            writeJson(w, errorResp("Invalid JSON.")); continue
                        }

                        val reqType = root.get("request")?.takeIf { it.isTextual }?.asText()
                        when (reqType) {
                            "put" -> handlePut(root, w)
                            "get" -> handleGet(root, ctx, w)
                            "delete" -> handleDelete(root, w)
                            "abort" -> handleAbort(root, ctx, w)
                            else -> writeJson(w, errorResp("Unrecognised request type."))
                        }
                    }
                } finally {
                    val requeued = core.onDisconnect(ctx)
                    if (requeued.isNotEmpty()) core.satisfyWaiters()
                    runCatching { w.flush() }
                    runCatching { sock.close() }
                }
            }
        }
    }

    private fun handlePut(root: JsonNode, w: BufferedWriter) {
        val qName = root.get("queue")?.takeIf { it.isTextual }?.asText()
            ?: return writeJson(w, errorResp("Missing/invalid 'queue'."))
        val pri = root.get("pri")?.takeIf { it.isInt && it.asInt() >= 0 }?.asInt()
            ?: return writeJson(w, errorResp("Missing/invalid 'pri'."))
        val jobNode = root.get("job")
            ?: return writeJson(w, errorResp("Missing 'job'."))
        if (!jobNode.isObject) return writeJson(w, errorResp("'job' must be an object."))

        val id = core.newId()
        val job = Job(id, qName, pri, jobNode.deepCopy())
        val picked = core.put(job)
        picked?.let { (waiter, j) ->
            if (!waiter.handoff.offer(j)) {
                core.satisfyWaiters()
            }
        }
        writeJson(w, okId(id))
    }

    private fun handleGet(root: JsonNode, ctx: ClientSession, w: BufferedWriter) {
        val arr = root.get("queues")
        if (arr == null || !arr.isArray || arr.isEmpty) {
            writeJson(w, errorResp("Missing/invalid 'queues'.")); return
        }
        val qNames = mutableListOf<String>()
        var bad = false
        for (n in arr) if (!n.isTextual) {
            bad = true; break
        } else qNames += n.asText()
        if (bad) {
            writeJson(w, errorResp("All 'queues' must be strings.")); return
        }

        val waitFlag = root.get("wait")?.takeIf { it.isBoolean }?.asBoolean() ?: false

        val immediate = core.tryGet(ctx, qNames)
        if (immediate != null) {
            writeJson(w, jobObj(immediate))
            return
        }

        if (!waitFlag) {
            writeJson(w, noJobResp()); return
        }

        val (job, waiter) = core.registerWait(ctx, qNames)
        if (job != null) {
            writeJson(w, jobObj(job))
            return
        }
        if (waiter != null) {
            val taken = waiter.handoff.take()
            ctx.waiter = null
            writeJson(w, jobObj(taken))
        }
    }

    private fun handleDelete(root: JsonNode, w: BufferedWriter) {
        val id = root.get("id")?.takeIf { it.isIntegralNumber }?.asLong()
            ?: return writeJson(w, errorResp("Missing/invalid 'id'."))

        if (core.delete(id)) {
            writeJson(w, okResp())
        } else {
            writeJson(w, noJobResp())
        }
    }

    private fun handleAbort(root: JsonNode, ctx: ClientSession, w: BufferedWriter) {
        val id = root.get("id")?.takeIf { it.isIntegralNumber }?.asLong()
            ?: return writeJson(w, errorResp("Missing/invalid 'id'."))
        val result = try {
            core.abort(ctx, id)
        } catch (_: JobCore.NotOwner) {
            "not-owner"
        }

        when (result) {
            null -> writeJson(w, noJobResp())
            true -> {
                writeJson(w, okResp());
            }

            "not-owner" -> writeJson(w, errorResp("Not owner of job $id."))
            else -> writeJson(w, errorResp("Unknown error."))
        }
    }


    private val mapper = ObjectMapper()

    private fun okId(id: Long) = mapOf("status" to "ok", "id" to id)
    private fun okResp() = mapOf("status" to "ok")
    private fun noJobResp() = mapOf("status" to "no-job")
    private fun errorResp(msg: String) = mapOf("status" to "error", "error" to msg)

    private fun jobObj(job: Job): ObjectNode =
        mapper.createObjectNode().apply {
            put("status", "ok")
            put("id", job.id)
            set<ObjectNode>("job", job.payload)
            put("pri", job.pri)
            put("queue", job.queue)
        }

    private fun writeJson(w: BufferedWriter, payload: Map<String, Any>) {
        w.write(mapper.writeValueAsString(payload))
        w.write('\n'.code)
        w.flush()
    }

    private fun writeJson(w: BufferedWriter, node: JsonNode) {
        w.write(mapper.writeValueAsString(node))
        w.write('\n'.code)
        w.flush()
    }
}
