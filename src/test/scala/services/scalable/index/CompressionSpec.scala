package services.scalable.index

import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class CompressionSpec extends AnyFlatSpec {

  "it " should "compress successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val os = new ByteArrayOutputStream()
    val factory = new CompressorStreamFactory()

    val lz4Out = factory
      .createCompressorOutputStream(CompressorStreamFactory.getLZ4Block, os).asInstanceOf[BlockLZ4CompressorOutputStream]

    val text = "blah blah blah blah blah blah"
    val input = new String(text).getBytes("UTF-8")

    lz4Out.write(input)
    lz4Out.flush()
    lz4Out.finish()

    logger.info(s"${input.length}, ${os.size()}")

    lz4Out.write(input)
    os.flush()
    os.close()

    val is = new ByteArrayInputStream(os.toByteArray)
    val lz4In = factory.createCompressorInputStream(CompressorStreamFactory.getLZ4Block, is)

    val output = lz4In.readAllBytes()

    val decompressed = new String(output, "UTF-8")

    logger.info(s"decompressed: $decompressed")

    assert(text.equals(decompressed))
  }

}
