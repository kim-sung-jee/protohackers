package com.example.sung_jee.p03

import com.example.sung_jee.Profiles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.text.Charsets.US_ASCII

@Profile(Profiles.PO3)
@Configuration
class BudgeChatClientStarter {
    @Bean(
        initMethod = "start",
        destroyMethod = "stop"
    )
    fun budgetChatServer() = BudgeChatServer(
        port = 7007,
        workerThreads = 10,
    )
}

class BudgeChatServer(
    private val port: Int,
    private val workerThreads: Int,
    private val boss: ExecutorService = Executors.newSingleThreadExecutor { r ->
        Thread(r, "budgetchat-boss")
    },
    private val workers: ExecutorService = Executors.newFixedThreadPool(workerThreads) { r ->
        Thread(r, "budgetchat-worker")
    }
) {
    private val running = AtomicBoolean(false)

    private val serverSocket: ServerSocket = ServerSocket(this.port)

    private val joined = ConcurrentHashMap<String, ChatClient>()

    fun start() {
        if (!running.compareAndSet(false, true)) return

        boss.execute {
            try {
                while (running.get()) {
                    val s = serverSocket.accept().apply { tcpNoDelay = true }
                    workers.execute { handleClient(s) }
                }
            } catch (_: Exception) {
            }
        }
    }

    fun stop() {
        running.set(false)
        try {
            serverSocket.close()
        } catch (_: Exception) {
        }
        boss.shutdownNow()
        workers.shutdownNow()
        workers.awaitTermination(2, TimeUnit.SECONDS)
    }

    private fun handleClient(sock: Socket) {
        val client = ChatClient(sock)
        try {
            client.sendLine("Welcome to budgetchat! What shall I call you?")

            val rawName = client.readLine() ?: return
            val name = rawName.trimEnd('\r')
            if (!isLegalName(name)) {
                client.sendLine("Illegal name. Use 1-16 alphanumerics.")
                return
            }

            val prev = joined.putIfAbsent(name, client)
            if (prev != null) {
                client.sendLine("That name is already in use.")
                return
            }
            client.joinedName = name

            val others = joined.keys.filter { it != name }
            client.sendLine(
                "* The room contains: ${others.joinToString(", ")}"
            )

            broadcastExcept(senderName = name, line = "* $name has entered the room")

            while (true) {
                val line = client.readLine() ?: break
                val msg = line.trimEnd('\r')
                if (msg.isEmpty()) continue
                val safe = if (msg.length > 4096) msg.substring(0, 4096) else msg
                broadcastExcept(senderName = name, line = "[$name] $safe")
            }
        } finally {
            val name = client.joinedName
            try {
                client.close()
            } catch (_: Exception) {
            }

            if (name != "") {
                val removed = joined.remove(name, client)
                if (removed) {
                    broadcastExcept(senderName = name, line = "* $name has left the room")
                }
            }
        }
    }

    private fun broadcastExcept(senderName: String, line: String) {
        joined.forEach { (name, cli) ->
            if (name == senderName) return@forEach
            cli.safeSend(line)
        }
    }

    private fun isLegalName(n: String): Boolean {
        if (n.isEmpty() || n.length > 16) return false
        return n.all { it.isLetterOrDigit() }
    }
}

class ChatClient(
    private val sock: Socket,
    var joinedName: String = "",
) {
    private val out = BufferedWriter(OutputStreamWriter(sock.getOutputStream(), US_ASCII))
    private val `in` = BufferedReader(InputStreamReader(sock.getInputStream(), US_ASCII))
    private val lock = Any()

    fun sendLine(s: String) = synchronized(lock) {
        out.write(s)
        out.write('\n'.code)
        out.flush()
    }

    fun safeSend(s: String) {
        try {
            sendLine(s)
        } catch (_: Exception) {
        }
    }

    fun readLine(): String? = `in`.readLine()
    fun close() {
        try {
            out.flush()
        } catch (_: Exception) {
        }
        try {
            sock.close()
        } catch (_: Exception) {
        }
    }
}