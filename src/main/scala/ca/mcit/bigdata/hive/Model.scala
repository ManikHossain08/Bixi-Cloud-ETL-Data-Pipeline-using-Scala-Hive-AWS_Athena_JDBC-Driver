package ca.mcit.bigdata.hive

case class StationInfo(data: StationList, last_updated: Int, ttl: Int)

case class StationList(stations: List[Stations])

case class Stations(
                     capacity: Int,
                     eightd_has_key_dispenser: Boolean,
                     eightd_station_services: Option[List[EightStationServices]],
                     electric_bike_surcharge_waiver: Boolean,
                     external_id: String,
                     has_kiosk: Boolean,
                     is_charging: Boolean,
                     lat: Double,
                     lon: Double,
                     rental_methods: List[String],
                     short_name: String,
                     station_id: String,
                     name: String
                   )


case class EightStationServices(
                                 id: String,
                                 bikes_availability: String,
                                 description: String,
                                 docks_availability: String,
                                 link_for_more_info: String,
                                 name: String,
                                 schedule_description: String,
                                 service_type: String
                               )

case class SystemInfo(data: System, last_updated: Int, ttl: Int)

case class System(
                   email: String,
                   language: String,
                   license_url: String,
                   name: String,
                   operator: String,
                   phone_number: String,
                   purchase_url: String,
                   short_name: String,
                   start_date: String,
                   system_id: String,
                   timezone: String,
                   url: String
                 )

/**
 * NOTE:
 * 1. Fields terminated by ','  =>  This is how each field is separated
 * 2. Collection items terminated by '|'  => this is how values in the struct, map and array are separated
 * 3. Multiple rows in List terminated by '#'  => This is how each row is separated from each other
 * 4. Lines terminated by '\n' stored as text file
 */

object Stations {
  val header: String = s"station_id,lon,lat,is_charging,external_id,capacity,eightd_has_key_dispenser," +
    s"electric_bike_surcharge_waiver,has_kiosk,rental_methods,short_name,eightd_station_services\n"

  def toCsv(in: Stations): String = {
    s"${in.station_id},${in.lon},${in.lat},${in.is_charging},${in.external_id},${in.capacity}," +
      s"${in.eightd_has_key_dispenser},${in.electric_bike_surcharge_waiver},${in.has_kiosk}," +
      s"${in.rental_methods.addString(new StringBuilder(), "|")},${in.short_name}," +
      s"${EightStationServices.getSAllServices(in.eightd_station_services)}\n"
  }
}

object EightStationServices {
  def getSAllServices(in: Option[List[EightStationServices]]): String = {
    var getAllStrRows = ""
    var rowNo = 1
    in match {
      case Some(in) =>
        val isLastRow = in.size
        in.foreach { service =>
          if (rowNo == isLastRow) getAllStrRows += toCsv(service)
          else getAllStrRows += toCsv(service) + "#"
          rowNo = rowNo + 1
        }
      case None => getAllStrRows += "|||||||"

    }
    getAllStrRows
  }

  def toCsv(in: EightStationServices): String = {
    s"${in.id}|${in.bikes_availability}|${in.description}|${in.docks_availability}|${in.link_for_more_info}|" +
      s"${in.name}|${in.schedule_description}|${in.service_type}"
  }
}

object System {
  val header: String = s"email,language,license_url,name,operator,phone_number,purchase_url,short_name,start_date," +
    s"system_id,timezone,url\n"

  def toCsv(in: System): String = {
    s"${in.email},${in.language},${in.license_url},${in.name},${in.operator},${in.phone_number},${in.purchase_url}," +
      s"${in.short_name},${in.start_date},${in.system_id},${in.timezone},${in.url}\n"
  }
}

