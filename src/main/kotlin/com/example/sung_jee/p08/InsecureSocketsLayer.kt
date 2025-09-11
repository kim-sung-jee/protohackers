package com.example.sung_jee.p08

import com.example.sung_jee.Profiles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.text.Charsets.US_ASCII

@Configuration
@Profile(Profiles.P08)
class InsecureSocketsLayerConfig {
    @Bean(initMethod = "start", destroyMethod = "stop")
    fun insecureSocketsLayer() = InsecureSocketsLayer(
        port = 7007,
        concurrency = 16,
    )
}

class InsecureSocketsLayer(
    private val port: Int,
    private val concurrency: Int,
    private val workers: ExecutorService = Executors.newFixedThreadPool(concurrency) { r ->
        Thread(
            r,
            "insecure-sockets-worker"
        )
    },
) {
    private val running = AtomicBoolean(false)

    @Volatile
    private var serverSocket: ServerSocket? = null

    class CipherSpec private constructor(val ops: List<Op>) {
        fun newSessionCodec(): Codec = Codec(ops)

        fun isNoOp(): Boolean {
            val codec = newSessionCodec()
            for (pos in 0..1) for (b in 0..255) {
                val enc = codec.ops.fold(b) { v, op -> op.enc(v, pos) } and 0xFF
                if (enc != (b and 0xFF)) return false
            }
            return true
        }

        class Codec(internal val ops: List<Op>) {

            var posIn: Int = 0
            var posOut: Int = 0

            fun encodeToClient(plain: Int): Int {
                var v = plain and 0xFF
                for (op in ops) v = op.enc(v, posOut)
                posOut++
                return v and 0xFF
            }

            fun decodeFromClient(enc: Int): Int {
                var v = enc and 0xFF
                for (i in ops.indices.reversed()) v = ops[i].dec(v, posIn)
                posIn++
                return v and 0xFF
            }
        }

        sealed interface Op {
            fun enc(b: Int, pos: Int): Int
            fun dec(b: Int, pos: Int): Int

            object ReverseBits : Op {
                override fun enc(b: Int, pos: Int) = reverseBits(b)
                override fun dec(b: Int, pos: Int) = reverseBits(b)
                private fun reverseBits(x: Int): Int {
                    var v = x and 0xFF
                    v = (v and 0xF0 ushr 4) or ((v and 0x0F) shl 4)
                    v = (v and 0xCC ushr 2) or ((v and 0x33) shl 2)
                    v = (v and 0xAA ushr 1) or ((v and 0x55) shl 1)
                    return v and 0xFF
                }
            }

            data class XorConst(val n: Int) : Op {
                override fun enc(b: Int, pos: Int) = (b xor n) and 0xFF
                override fun dec(b: Int, pos: Int) = (b xor n) and 0xFF
            }

            object XorPos : Op {
                override fun enc(b: Int, pos: Int) = (b xor (pos and 0xFF)) and 0xFF
                override fun dec(b: Int, pos: Int) = (b xor (pos and 0xFF)) and 0xFF
            }

            data class AddConst(val n: Int) : Op {
                override fun enc(b: Int, pos: Int) = (b + n) and 0xFF
                override fun dec(b: Int, pos: Int) = (b - n) and 0xFF
            }

            object AddPos : Op {
                override fun enc(b: Int, pos: Int) = (b + (pos and 0xFF)) and 0xFF
                override fun dec(b: Int, pos: Int) = (b - (pos and 0xFF)) and 0xFF
            }
        }

        companion object {
            fun parse(spec: ByteArray): CipherSpec? {
                val ops = mutableListOf<Op>()
                var i = 0
                while (i < spec.size) {
                    when ((spec[i].toInt() and 0xFF)) {
                        0x00 -> return CipherSpec(ops)
                        0x01 -> {
                            ops += Op.ReverseBits; i++
                        }

                        0x02 -> {
                            if (i + 1 >= spec.size) return null; ops += Op.XorConst(spec[i + 1].toInt() and 0xFF); i += 2
                        }

                        0x03 -> {
                            ops += Op.XorPos; i++
                        }

                        0x04 -> {
                            if (i + 1 >= spec.size) return null; ops += Op.AddConst(spec[i + 1].toInt() and 0xFF); i += 2
                        }

                        0x05 -> {
                            ops += Op.AddPos; i++
                        }
                    }
                }
                return null
            }
        }
    }

    fun start() {
        if (!running.compareAndSet(false, true)) return
        serverSocket = ServerSocket(port)
        while (running.get()) {
            val s = serverSocket!!.accept().apply { tcpNoDelay = true }
            workers.execute { handleClient(s) }
        }
    }

    fun stop() {
        running.set(false)
        serverSocket?.close()
    }

    private fun handleClient(sock: Socket) {
        BufferedInputStream(sock.getInputStream()).use { bin ->
            BufferedOutputStream(sock.getOutputStream()).use { bout ->
                val specBytes = readSpecUntilZero(bin, 80) ?: return
                val spec = CipherSpec.parse(specBytes) ?: return
                if (spec.isNoOp()) return

                val codec = spec.newSessionCodec()
                val lineBuf = StringBuilder(1024)

                while (true) {
                    val r = bin.read()
                    if (r == -1) break
                    val decB = codec.decodeFromClient(r)
                    val ch = decB.toChar()
                    lineBuf.append(ch)
                    if (ch == '\n') {
                        val request = lineBuf.toString().dropLast(1)
                        lineBuf.setLength(0)

                        val answer = pickMaxItem(request) ?: continue
                        val resp = "$answer\n"
                        for (bb in resp.toByteArray(US_ASCII)) {
                            val enc = codec.encodeToClient(bb.toInt())
                            bout.write(enc)
                        }
                        bout.flush()
                    }
                }
            }
        }
        runCatching { sock.close() }
    }

    private fun readSpecUntilZero(bin: BufferedInputStream, maxLen: Int): ByteArray? {
        val out = ArrayList<Int>(maxLen / Int.SIZE_BYTES)
        while (out.size < maxLen) {
            val b = bin.read()
            if (b == -1) return null
            out += b
            if ((b and 0xFF) == 0x00) break
        }
        if (out.isEmpty() || (out.last() and 0xFF) != 0x00) return null
        val arr = ByteArray(out.size)
        for (i in out.indices) arr[i] = (out[i] and 0xFF).toByte()
        return arr
    }

    private fun pickMaxItem(req: String): String? {
        // "10x toy car,15x dog on a string,4x inflatable motorcycle"
        var best: String? = null
        var bestN = Long.MIN_VALUE
        val items = req.split(',').map { it.trim() }.filter { it.isNotEmpty() }
        for (raw in items) {
            val xIdx = raw.indexOf('x')
            if (xIdx <= 0) continue
            val nStr = raw.substring(0, xIdx).trim()
            val n = nStr.toLongOrNull() ?: continue
            if (n > bestN) {
                bestN = n
                best = raw
            }
        }
        return best
    }

}

