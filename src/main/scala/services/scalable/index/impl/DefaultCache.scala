package services.scalable.index.impl

import com.github.benmanes.caffeine.cache.{Caffeine, RemovalCause}
import org.slf4j.LoggerFactory
import services.scalable.index._

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext

class DefaultCache[K, V](val MAX_BLOCK_CACHE_SIZE: Long = 100L * 1024L * 1024L,
                   val MAX_PARENT_ENTRIES: Int = 1000)
                  (implicit val ec: ExecutionContext) extends Cache[K,V] {

  val logger = LoggerFactory.getLogger(this.getClass)

  val blocks = Caffeine.newBuilder()
    .weigher[String, Block[K,V]]((key: String, value: Block[K,V]) => {
      if(value == null || value.isEmpty) 0 else value.size
    })
    .executor(ec.asInstanceOf[Executor])
    .maximumWeight(MAX_BLOCK_CACHE_SIZE)
    .removalListener((key: String, value: Block[K,V], cause: RemovalCause) => {
      logger.debug(s"REMOVING FROM BLOCKS CACHE ${key}... $cause\n")
    })
    .build[String, Block[K,V]]()

    val parents = Caffeine.newBuilder()
    .maximumSize(MAX_PARENT_ENTRIES)
    .executor(ec.asInstanceOf[Executor])
    .removalListener((key: String, value: (Option[String], Int), cause: RemovalCause) => {
      if(cause.wasEvicted())
        logger.debug(s"REMOVING FROM PARENTS CACHE ${key}... $cause\n")
    })
    .build[String, (Option[String], Int)]()

  def put(block: Block[K,V]): Unit = {
    blocks.put(block.unique_id, block)
  }

  def get(unique_id: String): Option[Block[K,V]] = {
    val block = blocks.getIfPresent(unique_id)
    if(block == null) None else Some(block)
  }

  override def put(unique_id: String, parent: Option[String], pos: Int): Unit = {
    parents.put(unique_id, parent -> pos)
  }

  override def getParent(unique_id: String): Option[(Option[String], Int)] = {
    val info = parents.getIfPresent(unique_id)
    if(info == null) None else Some(info)
  }

}
