package dmos.gae

import scala.language.implicitConversions

import com.google.appengine.{api => aeapi}
import Datastore.{Entity, Key, Kind}
import Blobstore.BlobKey

trait Objektas {
  val kind:Kind
  val key:Key
  def blobs:Set[BlobKey]
}

trait JuodasObjektas[O <: Objektas] {
  val kind:Kind
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
  def objektasFrom(e: Entity): O
}

object ObjektasFrom {
  def apply[O <: Objektas](vertejas: Entity => O) =
    new ObjektasFrom[O] { def objektasFrom(e: Entity) = vertejas(e) }
}

trait EntityFrom[O <: Objektas] {
  def entityFrom(o: O): Entity
}

object EntityFrom {
  def apply[O <: Objektas](vertejas: O => Entity) =
    new EntityFrom[O] { def entityFrom(o: O) = vertejas(o) }
}

trait EntityToFrom[O <: Objektas] {
  def objektasFrom(e:Entity):O
  def entityFrom(o:O):Entity
}

object EntityToFrom {
  implicit def entityToFrom[O <: Objektas](
    implicit objFrom:ObjektasFrom[O], entFrom:EntityFrom[O]
  ):EntityToFrom[O] =
    new EntityToFrom[O] {
      def objektasFrom(e:Entity) = objFrom.objektasFrom(e)
      def entityFrom(o:O) = entFrom.entityFrom(o)
  }
}

/* */

object Datastore {
  import aeapi.{datastore => ds}
  import scala.collection.JavaConverters._

  case class Kind(name:String) extends AnyVal

  type Key = ds.Key
  //type Entity = ds.Entity
  type Query = ds.Query

  trait Entity { 
    val under:ds.Entity

    private type Text = ds.Text

    private implicit def symbol2string(s:Symbol) = s.name

    def set[T](name:Symbol, value:T):Entity =
      { under.setProperty(name, value); this }

    def setUnindexedString(name:Symbol, value:String):Entity =
      set(name, new Text(value))

    def get[T](name:Symbol):T = under.getProperty(name).asInstanceOf[T]

    def getUnindexedString(name:Symbol):String = get[Text](name).getValue

    def optionGet[T](name:Symbol):Option[T] =
      if (under.hasProperty(name)) Some(get[T](name)) else None
  }

  object Entity {
    def apply(key:Key):Entity = Entity(new ds.Entity(key))

    def apply(ent:ds.Entity):Entity = new Entity {
      val under = ent
    }
  }
  
  object Query {
    def apply(kind:Kind):Query = new Query(kind.name)
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

  private implicit def entityDewrap(ent:Entity):ds.Entity = ent.under

  private implicit def entityWrap(ent:ds.Entity):Entity = Entity(ent)

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
  (kind:Kind, id:Long)(implicit conv:ObjektasFrom[O]):O =
    gauk[O](idToKey(kind, id))

  def trink(key:Key) = datastore.delete(key)

  def naujasKey(kind:Kind) = datastore.allocateIds(kind.name, 1).getStart

  def idToKey(kind:Kind, id:Long):Key =
    ds.KeyFactory.createKey(kind.name, id)

  def nameToKey(kind:Kind, name:String):Key =
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
