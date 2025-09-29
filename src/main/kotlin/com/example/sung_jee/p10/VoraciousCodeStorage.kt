package com.example.sung_jee.p10

import com.example.sung_jee.Profiles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.io.*
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets.US_ASCII
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Profile(Profiles.P10)
@Configuration
class VoraciousCodeStorageConfig {
    @Bean(initMethod = "start", destroyMethod = "stop")
    fun voraciousCodeStorage() = VoraciousCodeStorageServer(
        port = 7007,
        workerThreads = 64
    )
}

class VoraciousCodeStorageServer(
    private val port: Int,
    private val workerThreads: Int
) {
    private val running = AtomicBoolean(false)
    private var ss: ServerSocket? = null
    private val boss = Executors.newSingleThreadExecutor { r -> Thread(r, "vcs-boss") }
    private val pool = Executors.newFixedThreadPool(workerThreads) { r -> Thread(r, "vcs-wkr") }

    private val storage = Storage()
    private val nameRules = NameRules()
    private val textRules = TextRules()

    fun start() {
        if (!running.compareAndSet(false, true)) return
        ss = ServerSocket(port).apply { reuseAddress = true }
        boss.execute {
            while (running.get()) {
                val socket = ss!!.accept().apply {
                    tcpNoDelay = true
                    keepAlive = true
                }
                pool.execute { ConnectionHandler(socket, storage, nameRules, textRules).serve() }
            }
        }
    }

    fun stop() {
        running.set(false)
        runCatching { ss?.close() }
        boss.shutdownNow()
        pool.shutdownNow()
        pool.awaitTermination(2, TimeUnit.SECONDS)
    }
}

private class ConnectionHandler(
    private val sock: Socket,
    private val storage: Storage,
    private val nameRules: NameRules,
    private val textRules: TextRules
) {
    fun serve() {
        sock.use {
            val ins = BufferedInputStream(it.getInputStream())
            val outs = BufferedOutputStream(it.getOutputStream())
            val io = ProtocolIO(outs)
            val parser = CommandParser()

            io.ready()

            while (true) {
                val raw = IO.readLine(ins) ?: break;
                val line = raw.trimEnd()
                val cmd = parser.parse(line)

                when (cmd) {
                    is Command.Help -> {
                        io.writeLine("OK usage: HELP|GET|PUT|LIST")
                        io.ready()
                    }

                    is Command.Put -> {
                        val length = cmd.length.toIntOrNull()
                        if (length == null || length < 0) {
                            io.writeLine("ERR usage: PUT file length newline data"); io.ready(); continue
                        }
                        if (!nameRules.isValidFilePath(cmd.filename)) {
                            io.writeLine("ERR illegal file name"); io.ready(); continue
                        }
                        val body = if (length > 0) IO.readExact(ins, length) else ByteArray(0)
                        if (!textRules.isValidAsciiText(body)) {
                            io.writeLine("ERR text files only"); io.ready(); continue
                        }
                        val rev = storage.save(cmd.filename, body.toString(US_ASCII))
                        io.writeLine("OK r$rev")
                        io.ready()
                    }

                    is Command.Get -> {
                        if (!nameRules.isValidFilePath(cmd.filename)) {
                            io.writeLine("ERR illegal file name"); io.ready(); continue
                        }
                        val last = storage.lastRevision(cmd.filename)
                        if (last == null) {
                            io.writeLine("ERR no such file"); io.ready(); continue
                        }

                        val rev = if (cmd.revision == null) {
                            last
                        } else {
                            val tok = cmd.revision
                            if (!tok.startsWith("r")) {
                                io.writeLine("ERR no such revision"); io.ready(); continue
                            }
                            val n = tok.drop(1).toIntOrNull()
                            if (n == null) {
                                io.writeLine("ERR no such revision"); io.ready(); continue
                            }
                            n
                        }

                        val data = storage.read(cmd.filename, rev)
                        if (data == null) {
                            io.writeLine("ERR no such revision"); io.ready(); continue
                        }
                        val bytes = data.toByteArray(US_ASCII)
                        io.writeLine("OK ${bytes.size}"); io.writeRaw(data); io.ready()
                    }

                    is Command.List -> {
                        if (!nameRules.isValidDirPath(cmd.dir)) {
                            io.writeLine("ERR illegal dir name"); io.ready(); continue
                        }
                        val dir = if (cmd.dir.endsWith("/")) cmd.dir else cmd.dir + "/"
                        val rows = formatListRows(dir, storage)
                        io.writeLine("OK ${rows.size}")
                        rows.forEach { io.writeLine(it) }
                        io.ready()
                    }

                    is Command.Illegal -> {
                        io.writeLine("ERR illegal method: ${cmd.token}")
                        io.ready()
                    }

                    is Command.UsageError -> {
                        io.writeLine("ERR ${cmd.message}")
                        if (cmd.appendReady) io.ready()
                    }
                }
            }
        }
    }

    private fun formatListRows(dir: String, storage: Storage): List<String> {
        val all = storage.listRelative(dir).sortedBy { it.length }
        val result = linkedSetOf<String>()
        for (name in all) {
            if ('/' !in name) result += name else result += name.substringBefore('/') + "/"
        }
        return result.sorted().map { item ->
            if (item.endsWith("/")) "$item DIR"
            else "$item r${storage.lastRevision(dir + item)}"
        }
    }
}

private class Storage {
    private val meta = ConcurrentHashMap<String, Int>()
    private val store = ConcurrentHashMap<String, String>()
    private val locks = ConcurrentHashMap<String, Any>()
    private fun key(filename: String, rev: Int) = "$filename#$rev"

    fun save(filename: String, data: String): Int {
        val lock = locks.computeIfAbsent(filename) { Any() }
        synchronized(lock) {
            val last = meta[filename]
            if (last == null) {
                val r = 1
                store[key(filename, r)] = data
                meta[filename] = r
                return r
            }
            val lastData = store[key(filename, last)]
            if (data == lastData) return last
            val next = last + 1
            store[key(filename, next)] = data
            meta[filename] = next
            return next
        }
    }

    fun read(filename: String, rev: Int): String? = store[key(filename, rev)]
    fun lastRevision(filename: String): Int? = meta[filename]

    fun listRelative(dir: String): List<String> {
        require(dir.endsWith("/"))
        val uniqueFiles = store.keys
            .map { it.substringBefore('#') }
            .distinct()
            .filter { it.startsWith(dir) }
        return uniqueFiles.map { it.removePrefix(dir) }
    }
}

private class NameRules {
    private fun isValidFileName(seg: String): Boolean =
        seg.isNotEmpty() && seg.all { ch ->
            ch.isLetterOrDigit() || ch == '-' || ch == '_' || ch == '.'
        }

    fun isValidFilePath(input: String): Boolean {
        if (!input.startsWith("/")) return false
        val parts = input.split("/").drop(1)
        if (parts.isEmpty()) return false
        return parts.all(::isValidFileName)
    }

    fun isValidDirPath(input: String): Boolean {
        if (!input.startsWith("/")) return false
        var parts = input.split("/").drop(1)
        if (parts.isEmpty()) return true
        if (parts.last().isEmpty()) parts = parts.dropLast(1)
        return parts.all(::isValidFileName)
    }
}

private class TextRules {
    fun isValidAsciiText(buf: ByteArray): Boolean {
        for (b in buf) {
            val ub = b.toInt() and 0xFF
            if (ub == 0x0A || ub == 0x09 || (ub >= 0x20 && ub < 0x7F)) continue
            return false
        }
        return true
    }
}

private sealed interface Command {
    data object Help : Command
    data class Put(val filename: String, val length: String) : Command
    data class Get(val filename: String, val revision: String?) : Command
    data class List(val dir: String) : Command
    data class Illegal(val token: String) : Command
    data class UsageError(val message: String, val appendReady: Boolean) : Command
}

private class CommandParser {
    fun parse(line: String): Command {
        val parts = line.split(' ').filter { it.isNotEmpty() }
        if (parts.isEmpty()) return Command.UsageError("illegal method: ", appendReady = true)
        val raw = parts[0]
        val cmd = raw.lowercase(Locale.ROOT)
        return when (cmd) {
            "help" -> Command.Help
            "put" ->
                if (parts.size != 3) Command.UsageError("usage: PUT file length newline data", true)
                else Command.Put(parts[1], parts[2])

            "get" ->
                if (parts.size !in 2..3) Command.UsageError("usage: GET file [revision]", true)
                else Command.Get(parts[1], parts.getOrNull(2))

            "list" ->
                if (parts.size != 2) Command.UsageError("usage: LIST dir", true)
                else Command.List(parts[1])

            else -> Command.Illegal(raw)
        }
    }
}

private class ProtocolIO(private val outs: OutputStream) {
    fun writeLine(s: String) {
        outs.write(s.toByteArray(US_ASCII)); outs.write('\n'.code); outs.flush()
    }

    fun writeRaw(s: String) {
        outs.write(s.toByteArray(US_ASCII)); outs.flush()
    }

    fun ready() = writeLine("READY")
}

private object IO {
    fun readLine(ins: InputStream): String? {
        val baos = ByteArrayOutputStream()
        while (true) {
            val b = ins.read()
            if (b < 0) return if (baos.size() == 0) null else baos.toByteArray().toString(US_ASCII)
            baos.write(b)
            if (b == '\n'.code) break
        }
        return baos.toByteArray().toString(US_ASCII)
    }

    fun readExact(ins: InputStream, n: Int): ByteArray {
        val buf = ByteArray(n)
        var off = 0
        while (off < n) {
            val r = ins.read(buf, off, n - off)
            if (r < 0) throw EOFException("EOF while reading $n bytes (got $off)")
            off += r
        }
        return buf
    }
}
