package dmos.gae

import com.google.appengine.{api => aeapi}

case class Kindas(name:String) extends AnyVal

trait Objektas {
  import Datastore.{Entity, Key}
  import Blobstore.BlobKey
  val kind:Kindas
  val key:Key
  def blobs:Set[BlobKey]
}

trait JuodasObjektas[O <: Objektas] {
  import Datastore.Key
  val kind:Kindas
  def key:Key
  def issaugotas:O
  def svarus:O
}

trait Juodinamas[J] {
  objektas: Objektas =>
  def juodas:J
}

/* Entity to/from converteriai */

trait ObjektasFrom[O <: Objektas] {
  import Datastore.Entity
  def objektasFrom(e: Entity): O
}

object ObjektasFrom {
  import Datastore.Entity
  def apply[O <: Objektas](vertejas: Entity => O) =
    new ObjektasFrom[O] { def objektasFrom(e: Entity) = vertejas(e) }
}

trait EntityFrom[O <: Objektas] {
  import Datastore.Entity
  def entityFrom(o: O): Entity
}

object EntityFrom {
  import Datastore.Entity
  def apply[O <: Objektas](vertejas: O => Entity) =
    new EntityFrom[O] { def entityFrom(o: O) = vertejas(o) }
}

trait EntityToFrom[O <: Objektas] {
  import Datastore.Entity
  def objektasFrom(e:Entity):O
  def entityFrom(o:O):Entity
}

object EntityToFrom {
  implicit def entityToFrom[O <: Objektas](
    implicit objFrom:ObjektasFrom[O], entFrom:EntityFrom[O]
  ):EntityToFrom[O] =
    new EntityToFrom[O] {
      import Datastore.Entity
      def objektasFrom(e:Entity) = objFrom.objektasFrom(e)
      def entityFrom(o:O) = entFrom.entityFrom(o)
  }
}

/* */

object Datastore {
  import aeapi.{datastore => ds}
  import scala.collection.JavaConverters._

  type Key = ds.Key
  type Entity = ds.Entity
  type Query = ds.Query
  type Text = ds.Text
  
  object Query {
    def apply(kind:Kindas):Query = new Query(kind.name)
  }

  object Filter {
    def equal[T](name:String, value:T):ds.Query.FilterPredicate =
      ds.Query.FilterOperator.EQUAL.of(name, value)
  }

  private lazy val datastore = ds.DatastoreServiceFactory.getDatastoreService

  private lazy val defaultFetchOptions = ds.FetchOptions.Builder.withDefaults

  /*
  def entities(q:Query):List[Entity] =
    datastore.prepare(q)
      .asList(defaultFetchOptions)
      .asScala.toList
   */

  def gaukDaug[O <: Objektas]
  (q:Query)(implicit conv:ObjektasFrom[O]):List[O] =
    datastore.prepare(q)
      .asList(defaultFetchOptions)
      .asScala.toList
      .map(conv.objektasFrom(_))

  def issaugok[O <: Objektas]
  (obj:O)(implicit conv:EntityToFrom[O]):O = {
    val key = datastore.put(conv.entityFrom(obj))
    conv.objektasFrom(datastore.get(key))
  }

  def gauk[O <: Objektas]
  (key:Key)(implicit conv:ObjektasFrom[O]):O =
    conv.objektasFrom(datastore.get(key))

  def gauk[O <: Objektas]
  (kind:Kindas, id:Long)(implicit conv:ObjektasFrom[O]):O =
    gauk[O](idToKey(kind, id))

  def trink(key:Key) = datastore.delete(key)

  def naujasKey(kind:Kindas) = datastore.allocateIds(kind.name, 1).getStart

  def idToKey(kind:Kindas, id:Long):Key =
    ds.KeyFactory.createKey(kind.name, id)

  def nameToKey(kind:Kindas, name:String):Key =
    ds.KeyFactory.createKey(kind.name, name)
}

object Blobstore {
  import aeapi.{blobstore => bs}

  type BlobKey = bs.BlobKey

  private lazy val blobstore = bs.BlobstoreServiceFactory.getBlobstoreService

  def uploadUrl(successPath:String):String =
    blobstore.createUploadUrl(successPath)

  import javax.servlet.http.HttpServletRequest
  import scala.collection.JavaConverters._
  def uploads(req:HttpServletRequest):Map[String,BlobKey] =
    for( (k, vs) <- blobstore.getUploads(req).asScala.toMap )
      yield (k, vs.get(0))

  def baibai(blobKeys:Set[BlobKey]) = blobstore.delete(blobKeys.toSeq: _*)
}

object Images {
  import aeapi.images.{ImagesServiceFactory, ServingUrlOptions}
  import Blobstore.BlobKey

  private lazy val imagesService =
    ImagesServiceFactory.getImagesService()

  def url(blobKey:BlobKey):String =
    imagesService.getServingUrl(
      ServingUrlOptions.Builder.withBlobKey(blobKey).secureUrl(true))
}

object RichEntity {
  import Datastore.Entity

  implicit class RichEntity(val e:Entity) extends AnyVal {
    def get[T](name:String):T = e.getProperty(name).asInstanceOf[T]

    def optionGet[T](name:String):Option[T] =
      if (e.hasProperty(name)) Some(e.get[T](name)) else None
  }
}
