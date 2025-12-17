Class Lab7.REST.CarService Extends %CSP.REST
{

XData UrlMap [ XMLNamespace = "http://www.intersystems.com/urlmap" ]
{
<Routes>
    <Route Url="/cars" Method="GET" Call="GetAllCars"/>
    <Route Url="/car/:id" Method="GET" Call="GetCar"/>
    <Route Url="/car" Method="POST" Call="CreateCar"/>
    <Route Url="/car/:id" Method="PUT" Call="UpdateCar"/>
    <Route Url="/car/:id" Method="DELETE" Call="DeleteCar"/>
</Routes>
}

ClassMethod GetAllCars() As %Status
{
    Set array = ##class(%DynamicArray).%New()
    
    Set stmt = ##class(%SQL.Statement).%New()
    Set status = stmt.%Prepare("SELECT %ID, Vin, LicensePlate, Model->Brand, Model->ModelName, DailyRate, Status FROM Lab7.Car")
    
    If $$$ISERR(status) Quit ..ReportError(status)
    
    Set rset = stmt.%Execute()
    
    While rset.%Next() {
        Set obj = ##class(%DynamicObject).%New()
        Set obj.ID = rset.%Get("ID")
        Set obj.Vin = rset.%Get("Vin")
        Set obj.LicensePlate = rset.%Get("LicensePlate")
        Set obj.Brand = rset.%Get("Brand")
        Set obj.Model = rset.%Get("ModelName")
        Set obj.Rate = rset.%Get("DailyRate")
        Set obj.Status = rset.%Get("Status")
        
        Do array.%Push(obj)
    }
    
    Write array.%ToJSON()
    Quit $$$OK
}

ClassMethod GetCar(id As %String) As %Status
{
    If '##class(Lab7.Car).%ExistsId(id) {
        Set %response.Status = "404 Not Found"
        Write {"Error": "Car not found"}.%ToJSON()
        Quit $$$OK
    }
    
    Set car = ##class(Lab7.Car).%OpenId(id)
    
    Set obj = ##class(%DynamicObject).%New()
    Set obj.ID = car.%Id()
    Set obj.Vin = car.Vin
    Set obj.LicensePlate = car.LicensePlate
    Set obj.Rate = car.DailyRate
    Set obj.Status = car.Status
    
    If $IsObject(car.Model) {
        Set obj.Brand = car.Model.Brand
        Set obj.Model = car.Model.ModelName
    }
    
    Write obj.%ToJSON()
    Quit $$$OK
}

ClassMethod CreateCar() As %Status
{
    Try {
        Set data = ##class(%DynamicObject).%FromJSON(%request.Content)
        
        Set car = ##class(Lab7.Car).%New()
        Set car.Vin = data.Vin
        Set car.LicensePlate = data.LicensePlate
        Set car.DailyRate = data.Rate
        Set car.Status = data.Status
        
        Set brand = data.Brand
        Set modelName = data.Model
        
        &sql(SELECT ID INTO :modelId FROM Lab7.CarModel WHERE Brand = :brand AND ModelName = :modelName)
        If SQLCODE=0 {
            Set car.Model = ##class(Lab7.CarModel).%OpenId(modelId)
        } Else {
            Set newModel = ##class(Lab7.CarModel).%New()
            Set newModel.Brand = brand
            Set newModel.ModelName = modelName
            Do newModel.%Save()
            Set car.Model = newModel
        }
        
        Set sc = car.%Save()
        
        $$$ThrowOnError(sc)
        
        Set res = ##class(%DynamicObject).%New()
        Set res.Message = "Created successfully"
        Set res.ID = car.%Id()
        Write res.%ToJSON()
        
    } Catch ex {
        Set %response.Status = "500 Internal Server Error"
        Write {"Error": (ex.DisplayString())}.%ToJSON()
    }
    Quit $$$OK
}

ClassMethod UpdateCar(id As %String) As %Status
{
    If '##class(Lab7.Car).%ExistsId(id) {
        Set %response.Status = "404 Not Found"
        Write {"Error": "Car not found"}.%ToJSON()
        Quit $$$OK
    }
    
    Try {
        Set data = ##class(%DynamicObject).%FromJSON(%request.Content)
        Set car = ##class(Lab7.Car).%OpenId(id)
        
        If data.Vin'="" Set car.Vin = data.Vin
        If data.LicensePlate'="" Set car.LicensePlate = data.LicensePlate
        If data.Rate'="" Set car.DailyRate = data.Rate
        If data.Status'="" Set car.Status = data.Status
        
        Set sc = car.%Save()
        
        $$$ThrowOnError(sc)
        
        Write {"Message": "Updated successfully"}.%ToJSON()
        
    } Catch ex {
        Set %response.Status = "500 Internal Server Error"
        Write {"Error": (ex.DisplayString())}.%ToJSON()
    }
    Quit $$$OK
}

ClassMethod DeleteCar(id As %String) As %Status
{
    If '##class(Lab7.Car).%ExistsId(id) {
        Set %response.Status = "404 Not Found"
        Write {"Error": "Car not found"}.%ToJSON()
        Quit $$$OK
    }
    
    Set sc = ##class(Lab7.Car).%DeleteId(id)
    If $$$ISERR(sc) {
        Set %response.Status = "500 Internal Server Error"
        Write {"Error": ($System.Status.GetErrorText(sc))}.%ToJSON()
    } Else {
        Write {"Message": "Deleted successfully"}.%ToJSON()
    }
    Quit $$$OK
}

ClassMethod ReportError(sc As %Status) As %Status
{
    Set %response.Status = "500 Internal Server Error"
    Set error = ##class(%DynamicObject).%New()
    Set error.Error = $System.Status.GetErrorText(sc)
    Write error.%ToJSON()
    Quit $$$OK
}

}
