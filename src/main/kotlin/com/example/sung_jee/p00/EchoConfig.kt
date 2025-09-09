package com.example.sung_jee.p00

import com.example.sung_jee.Profiles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.ip.tcp.TcpInboundGateway
import org.springframework.integration.ip.tcp.connection.TcpNetServerConnectionFactory
import org.springframework.integration.ip.tcp.serializer.AbstractByteArraySerializer
import org.springframework.integration.ip.tcp.serializer.SoftEndOfStreamException
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream

import java.util.concurrent.Executors

@Profile(Profiles.P00)
@Configuration
class EchoConfig {
    @Bean
    fun tcpServerConnectionFactory(): TcpNetServerConnectionFactory =
        TcpNetServerConnectionFactory(7007).apply {
            serializer = ChunkingDeserializer()
            deserializer = ChunkingDeserializer()
            soTimeout = 60_000
            backlog = 50
            setTaskExecutor(Executors.newCachedThreadPool())
        }

    @Bean
    fun tcpInboundGateway(cf: TcpNetServerConnectionFactory): TcpInboundGateway =
        TcpInboundGateway().apply { setConnectionFactory(cf) }

    @Bean
    fun tcpEchoFlow(gw: TcpInboundGateway): IntegrationFlow =
        integrationFlow(gw) {
            handle<ByteArray> { payload, headers ->
                println("Received ${payload.size} bytes from ${headers["ip_address"]}:${headers["ip_port"]}")
                payload
            }
        }
}


class ChunkingDeserializer(
    private val maxChunkSize: Int = 8192
) : AbstractByteArraySerializer() {

    override fun deserialize(inputStream: InputStream): ByteArray {
        val first = inputStream.read()
        if (first == -1) throw SoftEndOfStreamException("Stream closed")

        val buf = ByteArrayOutputStream()
        buf.write(first)

        var toRead = inputStream.available().coerceAtMost(maxChunkSize - 1)
        while (toRead > 0) {
            val tmp = ByteArray(toRead)
            val n = inputStream.read(tmp, 0, toRead)
            if (n <= 0) break
            buf.write(tmp, 0, n)
            toRead = inputStream.available().coerceAtMost(maxChunkSize - buf.size())
        }
        return buf.toByteArray()
    }

    override fun serialize(payload: ByteArray, outputStream: OutputStream) {
        outputStream.write(payload)
        outputStream.flush()
    }
}