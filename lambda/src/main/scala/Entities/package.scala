/**
  * Created by k.neffati on 08/03/2017.
  */
package object Entities {
  case class User (timestamp_hour: Long,
                   place: String,
                   location: String,
                   person: String,
                   interst: String,
                   inputPropos:Map[String,String]=Map()
                  )
}
