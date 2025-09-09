package com.example.sung_jee.p06

import com.example.sung_jee.Profiles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets.US_ASCII
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean

typealias Road = Int
typealias Mile = Int
typealias SpeedLimit = Int
typealias Plate = String
typealias Timestamp = Long
typealias DayIndex = Int

typealias PlateSeries = TreeMap<Timestamp, Mile>
typealias RoadObservations = ConcurrentHashMap<Plate, PlateSeries>

@Profile(Profiles.P06)
@Configuration
class SpeedDaemonConfig {
    @Bean(initMethod = "start", destroyMethod = "stop")
    fun speedDaemon() = SpeedDaemonServer(port = 7007, workerThreads = 256)
}

class SpeedDaemonServer(
    private val port: Int,
    private val workerThreads: Int,
    private val boss: ExecutorService = Executors.newSingleThreadExecutor { r -> Thread(r, "spd-boss") },
    private val workers: ExecutorService = Executors.newFixedThreadPool(workerThreads) { r -> Thread(r, "spd-worker") },
    private val hbScheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(8) { r ->
        Thread(r, "spd-heartbeat")
    }
) {
    private val running = AtomicBoolean(false)

    private val SECONDS_PER_DAY: Long = 86_400L

    @Volatile
    private var serverSocket: ServerSocket? = null

    private val roadLimit = ConcurrentHashMap<Road, SpeedLimit>()

    private val observations = ConcurrentHashMap<Road, RoadObservations>()

    private val roadDispatchers = ConcurrentHashMap<Road, CopyOnWriteArrayList<ConnIO>>()

    private val pendingTickets = ConcurrentHashMap<Road, ArrayDeque<Ticket>>()

    private val ticketedDays = ConcurrentHashMap<Plate, MutableSet<DayIndex>>()

    fun start() {
        if (!running.compareAndSet(false, true)) return
        serverSocket = ServerSocket(port)
        boss.execute {
            try {
                while (running.get()) {
                    val s = serverSocket!!.accept().apply { tcpNoDelay = true }
                    workers.execute { handleClient(s) }
                }
            } catch (_: Exception) {
            } finally {
                try {
                    serverSocket?.close()
                } catch (_: Exception) {
                }
            }
        }
    }

    fun stop() {
        running.set(false)
        try {
            serverSocket?.close()
        } catch (_: Exception) {
        }
        boss.shutdownNow()
        workers.shutdownNow()
        hbScheduler.shutdownNow()
        workers.awaitTermination(2, TimeUnit.SECONDS)
    }

    private sealed interface Role {
        data class Camera(val road: Road, val mile: Mile, val limit: SpeedLimit) : Role
        data class Dispatcher(val roads: Set<Road>) : Role
    }

    private class ConnIO(val sock: Socket) {
        val din = DataInputStream(sock.getInputStream())
        val dout = DataOutputStream(sock.getOutputStream())
        val writeLock = Any()
        var hbTask: ScheduledFuture<*>? = null

        inline fun writePacket(
            flush: Boolean = true,
            block: DataOutputStream.() -> Unit
        ) {
            synchronized(writeLock) {
                dout.block()
                if (flush) dout.flush()
            }
        }

        fun close() {
            try {
                sock.close()
            } catch (_: Exception) {
            }
        }
    }

    private fun handleClient(sock: Socket) {
        val io = ConnIO(sock)
        var role: Role? = null
        var heartbeatSet = false
        try {
            while (true) {
                val msgType = readU8(io.din) ?: break
                when (msgType) {
                    0x20 -> { // Plate
                        val r = role
                        if (r !is Role.Camera) errorAndClose(io, "plate before camera")
                        val plate: Plate = readStr(io.din) ?: errorAndClose(io, "bad str")
                        val tsU32: Timestamp = readU32(io.din) ?: errorAndClose(io, "bad u32")
                        handlePlate(r, plate, tsU32)
                    }

                    0x40 -> { // WantHeartbeat
                        if (heartbeatSet) errorAndClose(io, "dup heartbeat")
                        val intervalDs = readU32(io.din)?.toInt() ?: errorAndClose(io, "bad u32")
                        heartbeatSet = true
                        if (intervalDs > 0) scheduleHeartbeat(io, intervalDs)
                    }

                    0x80 -> { // IAmCamera
                        if (role != null) errorAndClose(io, "role already set")
                        val road: Road = readU16(io.din) ?: errorAndClose(io, "bad road")
                        val mile: Mile = readU16(io.din) ?: errorAndClose(io, "bad mile")
                        val limit: SpeedLimit = readU16(io.din) ?: errorAndClose(io, "bad limit")
                        role = Role.Camera(road, mile, limit)
                        roadLimit.computeIfAbsent(road) { limit }
                    }

                    0x81 -> { // IAmDispatcher
                        if (role != null) errorAndClose(io, "role already set")
                        val n = readU8(io.din) ?: errorAndClose(io, "bad numroads")
                        val roads = LinkedHashSet<Road>()
                        repeat(n) {
                            roads.add(readU16(io.din) ?: errorAndClose(io, "bad road"))
                        }
                        role = Role.Dispatcher(roads)
                        roads.forEach { rd ->
                            roadDispatchers.computeIfAbsent(rd) { CopyOnWriteArrayList() }.add(io)
                            val q = pendingTickets[rd]
                            if (q != null) {
                                while (true) {
                                    val t = synchronized(q) { if (q.isEmpty()) null else q.removeFirst() } ?: break
                                    sendTicket(io, t)
                                }
                            }
                        }
                    }

                    else -> errorAndClose(io, "illegal msg type")
                }
            }
        } finally {
            io.hbTask?.cancel(true)
            if (role is Role.Dispatcher) {
                role.roads.forEach { roadDispatchers[it]?.remove(io) }
            }
            io.close()
        }
    }

    private fun handlePlate(cam: Role.Camera, plate: Plate, tsU32: Timestamp) {
        val (road, mile, limit) = cam
        val ts: Timestamp = tsU32

        val plateOnRoad: RoadObservations = observations.computeIfAbsent(road) { ConcurrentHashMap() }
        val plateSeries: PlateSeries = plateOnRoad.computeIfAbsent(plate) { TreeMap() }
        synchronized(plateSeries) {
            for ((ts2, mile2) in plateSeries) {
                val t1: Timestamp = minOf(ts, ts2)
                val t2: Timestamp = maxOf(ts, ts2)
                val m1: Mile = if (ts <= ts2) mile else mile2
                val m2: Mile = if (ts <= ts2) mile2 else mile
                val dt = (t2 - t1)
                if (dt <= 0) continue
                val distMiles = kotlin.math.abs(m2 - m1).toLong()
                if (distMiles == 0L) continue
                val speed100 = ((distMiles * 360000L) + dt / 2) / dt
                val overBy = speed100 - (limit * 100)
                if (overBy >= 50) {
                    val day1: DayIndex = (t1 / this.SECONDS_PER_DAY).toInt()
                    val day2: DayIndex = (t2 / this.SECONDS_PER_DAY).toInt()
                    val days = day1..day2
                    val set = ticketedDays.computeIfAbsent(plate) { Collections.synchronizedSet(mutableSetOf()) }
                    val clash = synchronized(set) {
                        if (days.asSequence().any { set.contains(it) }) {
                            true
                        } else {
                            set.addAll(days)
                            false
                        }
                    }
                    if (!clash) {
                        val ticket = Ticket(
                            plate = plate,
                            road = road,
                            mile1 = m1,
                            ts1 = t1,
                            mile2 = m2,
                            ts2 = t2,
                            speed100 = speed100.toInt()
                        )
                        dispatchOrQueue(ticket)
                    }
                }
            }
            plateSeries[ts] = mile
        }
    }

    private fun dispatchOrQueue(t: Ticket) {
        val dispatcher = roadDispatchers[t.road]?.firstOrNull()
        if (dispatcher != null && !dispatcher.sock.isClosed) {
            sendTicket(dispatcher, t)
            return
        }
        val q = pendingTickets.computeIfAbsent(t.road) { ArrayDeque() }
        synchronized(q) { q.addLast(t) }
    }

    private fun scheduleHeartbeat(io: ConnIO, intervalDeci: Int) {
        val periodMs = intervalDeci * 100L
        io.hbTask = hbScheduler.scheduleAtFixedRate({
            io.writePacket {
                writeByte(0x41)
            }
        }, periodMs, periodMs, TimeUnit.MILLISECONDS)
    }

    private fun sendTicket(io: ConnIO, t: Ticket) {
        io.writePacket(flush = false) {
            io.dout.writeByte(0x21)
            writeStr(io.dout, t.plate)
            writeU16(io.dout, t.road)
            writeU16(io.dout, t.mile1)
            writeU32(io.dout, t.ts1)
            writeU16(io.dout, t.mile2)
            writeU32(io.dout, t.ts2)
            writeU16(io.dout, t.speed100)
            io.dout.flush()
        }
    }

    private fun errorAndClose(io: ConnIO, msg: String): Nothing {
        io.writePacket {
            writeByte(0x10)
            writeStr(io.dout, msg)
        }
        io.close()
        throw RuntimeException("protocol error: $msg")
    }

    private data class Ticket(
        val plate: Plate,
        val road: Road,
        val mile1: Mile,
        val ts1: Timestamp,
        val mile2: Mile,
        val ts2: Timestamp,
        val speed100: Int,
    )
}

private fun readU8(din: DataInputStream): Int? = try {
    din.readUnsignedByte()
} catch (_: Exception) {
    null
}

private fun readU16(din: DataInputStream): Int? = try {
    din.readUnsignedShort()
} catch (_: Exception) {
    null
}

private fun readU32(din: DataInputStream): Timestamp? = try {
    din.readInt().toLong() and 0xFFFF_FFFFL
} catch (_: Exception) {
    null
}

private fun readStr(din: DataInputStream): String? {
    val len = readU8(din) ?: return null
    val buf = ByteArray(len)
    return try {
        din.readFully(buf)
        String(buf, US_ASCII)
    } catch (_: Exception) {
        null
    }
}

private fun writeU16(dout: DataOutputStream, v: Int) {
    dout.writeByte((v ushr 8) and 0xFF)
    dout.writeByte(v and 0xFF)
}

private fun writeU32(dout: DataOutputStream, v: Timestamp) {
    dout.writeByte(((v ushr 24) and 0xFF).toInt())
    dout.writeByte(((v ushr 16) and 0xFF).toInt())
    dout.writeByte(((v ushr 8) and 0xFF).toInt())
    dout.writeByte((v and 0xFF).toInt())
}

private fun writeStr(dout: DataOutputStream, s: String) {
    val bytes = s.toByteArray(US_ASCII)
    require(bytes.size <= 255) { "str too long" }
    dout.writeByte(bytes.size)
    dout.write(bytes)
}
