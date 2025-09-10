package com.example.sung_jee.p07

import com.example.sung_jee.Profiles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.US_ASCII
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.min

@Profile(Profiles.P07)
@Configuration
class LineReversal {
    @Bean(initMethod = "start", destroyMethod = "stop")
    fun lrcpServer() = LrcpLineReversalServer(
        port = 7007,
        rtoMillis = 3_000L,
        expiryMillis = 60_000L
    )
}

class LrcpLineReversalServer(
    private val port: Int,
    private val rtoMillis: Long,
    private val expiryMillis: Long,
    private val boss: ExecutorService = Executors.newSingleThreadExecutor { r -> Thread(r, "lrcp-recv") },
    private val timer: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(
            r,
            "lrcp-timer"
        )
    },
) {

    private data class OutSeg(val pos: Int, val bytes: ByteArray, var lastSendAt: Long)
    private class Session(
        val token: Int,
        val peer: InetSocketAddress,
    ) {
        var recvLen: Int = 0
        val inBuf = StringBuilder()

        var sndLen: Int = 0
        var lastAck: Int = 0
        val outSegs: MutableList<OutSeg> = mutableListOf()

        var lastHeard: Long = System.currentTimeMillis()
    }

    private val running = AtomicBoolean(false)
    private lateinit var sock: DatagramSocket
    private val sessions = ConcurrentHashMap<Int, Session>()

    fun start() {
        if (!running.compareAndSet(false, true)) return
        sock = DatagramSocket(port)
        boss.execute { recvLoop() }
        timer.scheduleAtFixedRate({ onTimer() }, 1_000, 1_000, TimeUnit.MILLISECONDS)
    }

    fun stop() {
        running.set(false)
        runCatching { sock.close() }
        boss.shutdownNow()
        timer.shutdownNow()
    }

    private fun recvLoop() {
        val buf = ByteArray(2048)
        while (running.get()) {
            val p = DatagramPacket(buf, buf.size)
            sock.receive(p)
            val raw = String(p.data, p.offset, p.length, US_ASCII)
            if (raw.length >= 1000) continue

            val msg = Msg.parse(raw) ?: continue
            when (msg) {
                is Msg.Connect -> onConnect(msg, p)
                is Msg.Data -> onData(msg, p)
                is Msg.Ack -> onAck(msg, p)
                is Msg.Close -> onClose(msg, p)
            }
        }
    }

    sealed interface Msg {
        data class Connect(val session: Int) : Msg
        data class Data(val session: Int, val pos: Int, val dataEscaped: String) : Msg
        data class Ack(val session: Int, val length: Int) : Msg
        data class Close(val session: Int) : Msg

        companion object {
            fun parse(raw: String): Msg? {
                val parts = splitFields(raw) ?: return null
                if (parts.isEmpty()) return null
                return when (parts[0]) {
                    "connect" -> parseConnect(parts)
                    "data" -> parseData(parts)
                    "ack" -> parseAck(parts)
                    "close" -> parseClose(parts)
                    else -> null
                }
            }

            fun encode(msg: Msg): String = when (msg) {
                is Connect -> "/connect/${msg.session}/"
                is Data -> "/data/${msg.session}/${msg.pos}/${msg.dataEscaped}/"
                is Ack -> "/ack/${msg.session}/${msg.length}/"
                is Close -> "/close/${msg.session}/"
            }

            private fun splitFields(raw: String): List<String>? {
                if (raw.length < 2 || raw.first() != '/' || raw.last() != '/') return null
                val body = raw.substring(1, raw.length - 1)

                val parts = mutableListOf<String>()
                val sb = StringBuilder()
                var esc = false
                for (ch in body) {
                    when {
                        esc -> {
                            sb.append(ch); esc = false
                        }

                        ch == '\\' -> esc = true
                        ch == '/' -> {
                            parts += sb.toString(); sb.setLength(0)
                        }

                        else -> sb.append(ch)
                    }
                }
                if (esc) return null
                if (sb.isNotEmpty()) parts += sb.toString()
                return parts
            }

            private fun parseNum(s: String): Int? {
                if (s.isEmpty() || s.any { it !in '0'..'9' }) return null
                val v = s.toLongOrNull() ?: return null
                return if (v in 0L..Int.MAX_VALUE) v.toInt() else null
            }

            private fun List<String>.intAt(i: Int): Int? =
                getOrNull(i)?.let(::parseNum)

            private fun parseConnect(p: List<String>): Msg? {
                if (p.size != 2) return null
                val ses = p.intAt(1) ?: return null
                return Connect(ses)
            }

            private fun parseData(p: List<String>): Msg? {
                if (p.size != 4) return null
                val ses = p.intAt(1) ?: return null
                val pos = p.intAt(2) ?: return null
                return Data(ses, pos, p[3])
            }


            private fun parseAck(p: List<String>): Msg? {
                if (p.size != 3) return null
                val ses = p.intAt(1) ?: return null
                val len = p.intAt(2) ?: return null
                return Ack(ses, len)
            }


            private fun parseClose(p: List<String>): Msg? {
                if (p.size != 2) return null
                val ses = p.intAt(1) ?: return null
                return Close(ses)
            }
        }
    }

    private fun onConnect(m: Msg.Connect, p: DatagramPacket) {
        val now = System.currentTimeMillis()
        val addr = InetSocketAddress(p.address, p.port)
        val ses = sessions.computeIfAbsent(m.session) { Session(m.session, addr) }
        ses.lastHeard = now
        sendAck(ses, 0)
    }

    private fun onData(m: Msg.Data, p: DatagramPacket) {
        val ses = sessions[m.session]
        if (ses == null) {
            sendClose(m.session, InetSocketAddress(p.address, p.port))
            return
        }
        ses.lastHeard = System.currentTimeMillis()
        if (ses.recvLen != m.pos) {
            sendAck(ses, ses.recvLen)
            return
        }
        val payload = unescapeData(m.dataEscaped)
        ses.recvLen += payload.size
        sendAck(ses, ses.recvLen)
        responseReversedLine(ses, payload)
    }

    private fun onAck(m: Msg.Ack, p: DatagramPacket) {
        val ses = sessions[m.session]
        if (ses == null) {
            sendClose(m.session, InetSocketAddress(p.address, p.port))
            return
        }
        ses.lastHeard = System.currentTimeMillis()

        val L = m.length
        if (L <= ses.lastAck) return
        if (L > ses.sndLen) {
            sendClose(ses.token, ses.peer)
            sessions.remove(ses.token)
            return
        }

        ses.lastAck = L
        synchronized(ses) {
            ses.outSegs.removeIf { (it.pos + it.bytes.size) <= ses.lastAck }
        }
        if (L < ses.sndLen) {
            synchronized(ses) {
                ses.outSegs.forEach { seg ->
                    if (seg.pos >= L) sendOutSeg(ses, seg)
                }
            }
        }
    }

    private fun onClose(m: Msg.Close, p: DatagramPacket) {
        val addr = InetSocketAddress(p.address, p.port)
        sendClose(m.session, addr)
        sessions.remove(m.session)
    }

    private fun responseReversedLine(ses: Session, bytes: ByteArray) {
        val s = String(bytes, US_ASCII)
        ses.inBuf.append(s)
        while (true) {
            val idx = ses.inBuf.indexOf("\n")
            if (idx < 0) break
            val line = ses.inBuf.substring(0, idx)
            ses.inBuf.delete(0, idx + 1)
            val out = line.reversed() + "\n"
            enqueueSend(ses, out.toByteArray(US_ASCII))
        }
    }

    private fun sendAck(ses: Session, length: Int) {
        val msg = Msg.encode(Msg.Ack(ses.token, length))
        sendRaw(msg, ses.peer)
    }

    private fun sendClose(session: Int, peer: InetSocketAddress) {
        val msg = Msg.encode(Msg.Close(session))
        sendRaw(msg, peer)
    }

    private fun enqueueSend(ses: Session, payload: ByteArray) {
        var off = 0
        while (off < payload.size) {
            val take = min(300, payload.size - off)
            val chunk = payload.copyOfRange(off, off + take)
            val pos = ses.sndLen
            ses.sndLen += chunk.size
            val seg = OutSeg(pos, chunk, 0L)
            synchronized(ses) { ses.outSegs.add(seg) }
            sendOutSeg(ses, seg)
            off += take
        }
    }

    private fun sendOutSeg(ses: Session, seg: OutSeg) {
        val escaped = escapeData(seg.bytes)
        val msg = Msg.encode(Msg.Data(ses.token, seg.pos, escaped))
        sendRaw(msg, ses.peer)
        seg.lastSendAt = System.currentTimeMillis()
    }

    private fun sendRaw(s: String, peer: InetSocketAddress) {
        val bytes = s.toByteArray(US_ASCII)
        if (bytes.size >= 1000) {
            return
        }
        val p = DatagramPacket(bytes, bytes.size, peer)
        runCatching { sock.send(p) }
    }


    private fun onTimer() {
        val now = System.currentTimeMillis()
        val it = sessions.values.iterator()
        while (it.hasNext()) {
            val ses = it.next()
            if (now - ses.lastHeard >= expiryMillis) {
                sendClose(ses.token, ses.peer)
                it.remove()
                continue
            }
            synchronized(ses) {
                for (seg in ses.outSegs) {
                    if ((seg.pos + seg.bytes.size) <= ses.lastAck) continue
                    if (now - seg.lastSendAt >= rtoMillis) {
                        sendOutSeg(ses, seg)
                    }
                }
            }
        }
    }

    private fun unescapeData(s: String): ByteArray {
        val out = StringBuilder(s.length)
        var i = 0
        while (i < s.length) {
            val c = s[i]
            if (c == '\\' && i + 1 < s.length) {
                val n = s[i + 1]
                if (n == '/' || n == '\\') {
                    out.append(n)
                    i += 2
                    continue
                }
            }
            out.append(c)
            i++
        }
        return out.toString().toByteArray(US_ASCII)
    }

    private fun escapeData(b: ByteArray): String {
        val s = String(b, US_ASCII)
        val out = StringBuilder(s.length)
        for (ch in s) {
            when (ch) {
                '/' -> {
                    out.append("\\/")
                }

                '\\' -> {
                    out.append("\\\\")
                }

                else -> out.append(ch)
            }
        }
        return out.toString()
    }
}
