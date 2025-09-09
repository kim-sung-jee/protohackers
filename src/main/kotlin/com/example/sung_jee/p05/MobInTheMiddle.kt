package com.example.sung_jee.mitm

import com.example.sung_jee.Profiles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets.US_ASCII
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Profile(Profiles.P05)
@Configuration
class MitmProxyConfig {
    @Bean(initMethod = "start", destroyMethod = "stop")
    fun mobInTheMiddle() = MobInTheMiddleProxy(
        listenPort = 7007,
        workerThreads = 32
    )
}


internal fun BufferedReader.readLineStrict(): String? {
    val sb = StringBuilder()
    while (true) {
        val ch = this.read()
        if (ch == -1) return null
        if (ch == '\n'.code) {
            if (sb.isNotEmpty() && sb.last() == '\r') sb.setLength(sb.length - 1)
            return sb.toString()
        }
        sb.append(ch.toChar())
    }
}

class MobInTheMiddleProxy(
    private val listenPort: Int,
    private val workerThreads: Int,
    private val boss: ExecutorService = Executors.newSingleThreadExecutor { r -> Thread(r, "mitm-boss") },
    private val workers: ExecutorService = Executors.newFixedThreadPool(workerThreads) { r ->
        Thread(
            r,
            "mitm-worker"
        )
    },
    private val upstreamHost: String = "chat.protohackers.com",
    private val upstreamPort: Int = 16963,
) {
    private val running = AtomicBoolean(false)

    @Volatile
    private var serverSocket: ServerSocket? = null

    companion object {
        private val BOGUS_REGEX = Regex("(^| )(7[0-9A-Za-z]{25,34})(?=$| )")
        private const val TONY = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
    }

    fun start() {
        if (!running.compareAndSet(false, true)) return
        val ss = ServerSocket(listenPort).also {
            serverSocket = it
        }

        boss.execute {
            try {
                while (running.get()) {
                    val downstream = ss.accept().apply { tcpNoDelay = true }
                    workers.execute { handleOneClient(downstream) }
                }
            } catch (_: Exception) {
            }
        }
    }

    fun stop() {
        running.set(false)
        serverSocket?.close()
        boss.shutdownNow()
        workers.shutdownNow()
        workers.awaitTermination(2, TimeUnit.SECONDS)
    }

    private fun handleOneClient(down: Socket) {
        Socket().apply {
            tcpNoDelay = true
            connect(InetSocketAddress(upstreamHost, upstreamPort), 5000)
        }.use { up ->
            val f1 = CompletableFuture.runAsync({ pumpLines(down, up, "C→S") }, workers)
            val f2 = CompletableFuture.runAsync({ pumpLines(up, down, "S→C") }, workers)
            CompletableFuture.anyOf(f1, f2).join()
            up.close()
            down.close()
        }
    }

    private fun pumpLines(src: Socket, dst: Socket, dir: String) {
        BufferedReader(InputStreamReader(src.getInputStream(), US_ASCII)).use { r ->
            BufferedWriter(OutputStreamWriter(dst.getOutputStream(), US_ASCII)).use { w ->
                while (true) {
                    val line = r.readLineStrict() ?: break
                    val rewritten = rewriteBogus(line)
                    w.write(rewritten)
                    w.write('\n'.code)
                    w.flush()
                }
            }
        }
    }

    private fun rewriteBogus(s: String): String =
        BOGUS_REGEX.replace(s) { m ->
            val prefix = m.groupValues[1]
            prefix + TONY
        }
}
