package com.example.sung_jee.p02

import com.example.sung_jee.Profiles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


@Profile(Profiles.P02)
@Configuration
class MeansToAnEnd {
    @Bean(
        initMethod = "start",
        destroyMethod = "stop"
    )
    fun meansToAndEndServer() = MeansToAnEndServer(7007, 5)
}

class MeansToAnEndServer(
    private val port: Int,
    private val nThreads: Int,
    private val boss: ExecutorService = Executors.newSingleThreadExecutor(),
    private val workers: ExecutorService = Executors.newFixedThreadPool(nThreads)
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

    private fun beIntAt(b: ByteArray, off: Int): Int =
        ((b[off].toInt() and 0xFF) shl 24) or
                ((b[off + 1].toInt() and 0xFF) shl 16) or
                ((b[off + 2].toInt() and 0xFF) shl 8) or
                (b[off + 3].toInt() and 0xFF)

    private fun handleClient(sock: Socket) {
        sock.tcpNoDelay = true
        val book = TreeMap<Int, Int>()

        DataInputStream(sock.getInputStream().buffered()).use { din ->
            DataOutputStream(sock.getOutputStream().buffered()).use { dout ->
                val buf = ByteArray(9)
                while (true) {
                    try {
                        din.readFully(buf)
                    } catch (_: EOFException) {
                        break
                    }

                    val t = buf[0].toInt().toChar()
                    val a = beIntAt(buf, 1)
                    val b = beIntAt(buf, 5)

                    when (t) {
                        'I' -> book[a] = b
                        'Q' -> {
                            val mean = queryMean(book, a, b)
                            dout.writeInt(mean) // DataOutputStream은 Big Endian
                            dout.flush()
                        }

                        else -> break
                    }
                }
            }
        }
        try {
            sock.close()
        } catch (_: Exception) {
        }
    }

    private fun queryMean(book: TreeMap<Int, Int>, min: Int, max: Int): Int {
        if (min > max) return 0
        val sub = book.subMap(min, true, max, true)
        if (sub.isEmpty()) return 0
        var sum = 0L
        var cnt = 0
        for (p in sub.values) {
            sum += p.toLong()
            cnt++
        }
        return (sum / cnt).toInt()
    }
}

