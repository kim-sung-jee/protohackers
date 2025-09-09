package com.example.sung_jee.p01

import com.example.sung_jee.Profiles
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.math.BigInteger
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Profile(Profiles.P01)
@Configuration
class PrimeServerConfig {
    @Bean(initMethod = "start", destroyMethod = "stop")
    fun primeServer() = PrimeServer(7007, 16)
}


class PrimeServer(
    private val port: Int,
    private val nThreads: Int,
    private val mapper: ObjectMapper = ObjectMapper(),
    private val boss: ExecutorService = Executors.newSingleThreadExecutor(),
    private val workers: ExecutorService = Executors.newFixedThreadPool(nThreads),
    private val TWO: BigInteger = BigInteger.valueOf(2)
) {
    @Volatile
    private var serverSocket: ServerSocket = ServerSocket(this.port)

    fun start() {
        boss.execute {
            try {
                while (!Thread.currentThread().isInterrupted) {
                    val s = serverSocket.accept()
                    workers.execute { handleClient(s) }
                }
            } catch (_: Exception) {
            }
        }
    }

    fun stop() {
        try {
            serverSocket.close()
        } catch (_: Exception) {
        }
        boss.shutdownNow()
        workers.shutdownNow()
        workers.awaitTermination(2, TimeUnit.SECONDS)
    }

    private fun handleClient(sock: Socket) {
        sock.tcpNoDelay = true
        PrintWriter(sock.getOutputStream().buffered(), false).use { w ->
            BufferedReader(InputStreamReader(sock.getInputStream())).use { r ->
                while (true) {
                    val line = r.readLine() ?: break
                    val ok = processOne(line, w)
                    if (!ok) break
                }
            }
        }
        try {
            sock.close()
        } catch (_: Exception) {
        }
    }

    private fun processOne(line: String, w: PrintWriter): Boolean {
        val node: JsonNode = try {
            mapper.readTree(line)
        } catch (_: Exception) {
            sendMalformed(w); return false
        }
        if (!node.isObject) {
            sendMalformed(w); return false
        }

        val method = node.get("method") ?: run { sendMalformed(w); return false }
        val number = node.get("number") ?: run { sendMalformed(w); return false }
        if (!method.isTextual || method.asText() != "isPrime") {
            sendMalformed(w); return false
        }
        if (!number.isNumber) {
            sendMalformed(w); return false
        }

        val prime = if (!number.isIntegralNumber) {
            false
        } else {
            val n = try {
                number.bigIntegerValue()
            } catch (_: Exception) {
                BigInteger(number.asText())
            }
            isPrime(n)
        }

        w.println("""{"method":"isPrime","prime":${if (prime) "true" else "false"}}""")
        w.flush()
        return true
    }

    private fun isPrime(n: BigInteger): Boolean {
        if (n < TWO) return false
        return n.isProbablePrime(10)
    }

    private fun sendMalformed(w: PrintWriter) {
        w.println("malformed")
        w.flush()
    }
}