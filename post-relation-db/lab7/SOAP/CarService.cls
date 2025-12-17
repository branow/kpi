Class Lab7.SOAP.CarService Extends %SOAP.WebService
{

/// Назва сервісу
Parameter SERVICENAME = "CarRentSOAP";

/// Унікальний простір імен XML
Parameter NAMESPACE = "http://Lab7.org";

/// Використовувати назви класів у XML схемі
Parameter USECLASSNAMESPACES = 1;

/// 1. Створення авто
/// Повертає ID створеного авто або повідомлення про помилку
Method CreateCar(Vin As %String, LicensePlate As %String, Rate As %Decimal, Brand As %String, ModelName As %String, Status As %String) As %String [ WebMethod ]
{
    Try {
        Set modelId = ""
        &sql(SELECT ID INTO :modelId FROM Lab7.CarModel WHERE Brand = :Brand AND ModelName = :ModelName)
        
        Set modelObj = ""
        If SQLCODE=0 {
            Set modelObj = ##class(Lab7.CarModel).%OpenId(modelId)
        } Else {
            Set modelObj = ##class(Lab7.CarModel).%New()
            Set modelObj.Brand = Brand
            Set modelObj.ModelName = ModelName
            Do modelObj.%Save()
        }

        Set car = ##class(Lab7.Car).%New()
        Set car.Vin = Vin
        Set car.LicensePlate = LicensePlate
        Set car.DailyRate = Rate
        Set car.Status = Status
        Set car.Model = modelObj
        
        Do car.%Save()
        
        Return car.%Id()
        
    } Catch ex {
        Return "Error: " _ ex.DisplayString()
    }
}

/// 2. Отримання авто по ID
/// Повертає об'єкт Lab7.Car. IRIS автоматично перетворить його в XML.
Method GetCar(id As %String) As Lab7.Car [ WebMethod ]
{
    If '##class(Lab7.Car).%ExistsId(id) Quit ""
    Quit ##class(Lab7.Car).%OpenId(id)
}

/// 3. Оновлення авто
Method UpdateCar(id As %String, Rate As %Decimal, Status As %String) As %String [ WebMethod ]
{
    If '##class(Lab7.Car).%ExistsId(id) Quit "Error: Not Found"
    
    Set car = ##class(Lab7.Car).%OpenId(id)
    If Rate'="" Set car.DailyRate = Rate
    If Status'="" Set car.Status = Status
    
    Do car.%Save()
    Quit "Updated"
}

/// 4. Видалення авто
Method DeleteCar(id As %String) As %String [ WebMethod ]
{
    If '##class(Lab7.Car).%ExistsId(id) Quit "Error: Not Found"
    
    Do ##class(Lab7.Car).%DeleteId(id)
    Quit "Deleted"
}

}
