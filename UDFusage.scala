import org.apache.spark.sql.functions.udf

object UDFusage {

  val replaceM: (String) => String = {
    case txt@"1" => txt.replace("1", "January")
    case txt@"2" => txt.replace("2", "February")
    case txt@"3" => txt.replace("3", "March")
    case txt@"4" => txt.replace("4", "April")
    case txt@"5" => txt.replace("5", "May")
    case txt@"6" => txt.replace("6", "June")
    case txt@"7" => txt.replace("7", "July")
    case txt@"8" => txt.replace("8", "August")
    case txt@"9" => txt.replace("9", "September")
    case txt@"10" => txt.replace("10", "October")
    case txt@"11" => txt.replace("11", "November")
    case txt@"12" => txt.replace("12", "December")
  }
  def changeMtoM = udf(replaceM)

  val replaceD: (String) => String = {
    case txt@"1" => txt.replace("1", "Monday")
    case txt@"2" => txt.replace("2", "Tuesday")
    case txt@"3" => txt.replace("3", "Wednesday")
    case txt@"4" => txt.replace("4", "Thursday")
    case txt@"5" => txt.replace("5", "Friday")
    case txt@"6" => txt.replace("6", "Saturday")
    case txt@"7" => txt.replace("7", "Sunday")
  }
  def changeDtoD = udf(replaceD)

  val convert: (String) => Int = (txt: String) => {

    (txt.toInt / 100).toInt * 60 + (txt.toInt % 100)

  }

  def converttoTotalMin = udf(convert)
  val isHolidayF:(Int, Int) => String = (month: Int, day: Int) =>
  {
    // We consider Christmas to range from Dec 22th to Jan 7th
    if ((month == 12 && day > 22) || (month == 1 && day < 7))
      "Christmas"
    else /*if (month == 3 && day > 22) //we removed it because the date changes every year
           "Easter"
            else*/ if (month == 7 || month == 8)
      "Vacation"
    else
      "Normal"
  }
  def isHoliday = udf(isHolidayF)

  val getRushHourF: Int => String = (time : Int) =>
  {
    if (time < 660) // 660 min = 11:00 am
      "Low"
    else if (time >= 1020 && time <= 1260) // 16:00 - 21:00
      "High"
    else
      "Medium"
  }
  def getRushHour = udf(getRushHourF)


}
