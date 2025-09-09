package com.example.sung_jee.p04

import com.example.sung_jee.Profiles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Profile(Profiles.P04)
@Configuration
class UnusualDatabase {
    @Bean(initMethod = "start", destroyMethod = "stop")
    fun unusualDatabaseProgram() = UnusualDatabaseProgram(7007, 16)
}

class UnusualDatabaseProgram(
    private val port: Int,
    private val workerThreads: Int,
    private val workers: ExecutorService = Executors.newFixedThreadPool(workerThreads) { r ->
        Thread(r, "udp-kv-worker")
    }
) {
    private val running = AtomicBoolean(false)

    @Volatile
    private var socket: DatagramSocket? = null
    private val store = ConcurrentHashMap<String, String>()
    private val CS: Charset = StandardCharsets.ISO_8859_1
    private val VERSION_VALUE = "SungJee UDP KV 1.0"

    private val MAX_WIRE = 1000

    fun start() {
        if (!running.compareAndSet(false, true)) return
        val s = DatagramSocket(port).also { socket = it }
        println("[udp-kv] listening on udp/$port")

        Thread({
            val buf = ByteArray(2048)
            try {
                while (running.get()) {
                    val pkt = DatagramPacket(buf, buf.size)
                    s.receive(pkt) // blocking
                    val data = pkt.data.copyOfRange(pkt.offset, pkt.offset + pkt.length)
                    val remote = InetSocketAddress(pkt.address, pkt.port)
                    workers.execute { handlePacket(data, s, remote) }
                }
            } catch (_: Exception) {
            }
        }, "udp-kv-boss").start()
    }

    fun stop() {
        running.set(false)
        try {
            socket?.close()
        } catch (_: Exception) {
        }
        workers.shutdownNow()
        workers.awaitTermination(2, TimeUnit.SECONDS)
        println("[udp-kv] stopped")
    }

    private fun handlePacket(data: ByteArray, sock: DatagramSocket, remote: InetSocketAddress) {
        if (data.size >= MAX_WIRE) return

        val eq = indexOfEq(data)
        if (eq >= 0) {
            val key = String(data, 0, eq, CS)
            if (key == "version") return

            val value = String(data, eq + 1, data.size - (eq + 1), CS)
            store[key] = value
            return
        } else {
            val key = String(data, CS)
            val value =
                if (key == "version") VERSION_VALUE
                else store.getOrDefault(key, "")

            val resp = "$key=$value"
            val out = resp.toByteArray(CS)
            if (out.size >= MAX_WIRE) {
                return
            }
            val pkt = DatagramPacket(out, out.size, remote.address, remote.port)
            sock.send(pkt)
        }
    }

    private fun indexOfEq(arr: ByteArray): Int {
        val eq = 0x3D.toByte() // '='
        for (i in arr.indices) if (arr[i] == eq) return i
        return -1
    }
}