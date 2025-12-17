Class Lab6.Run Extends %RegisteredObject
{

ClassMethod Setup(count As %Integer = 10) As %Status
{
    Do ##class(Lab6.CarModel).Populate(count)
    Write "Created ", count, " CarModels", !

    Do ##class(Lab6.Car).Populate(count)
    Write "Created ", count, " Cars", !

    Do ##class(Lab6.Customer).Populate(count)
    Write "Created ", count, " Customers", !

    Do ##class(Lab6.Employee).Populate(count)
    Write "Created ", count, " Employees", !

    Do ##class(Lab6.RentalContract).Populate(count)
    Write "Created ", count, " RentalContracts", !

    Do ##class(Lab6.Payment).Populate(count)
    Write "Created ", count, " Payments", !

    Write !, "Demo Data Setup Complete", !
    Quit $$$OK
}

ClassMethod Reset() As %Status
{
    Do ##class(Lab6.Payment).%DeleteExtent()
    Do ##class(Lab6.RentalContract).%DeleteExtent()
    Do ##class(Lab6.Car).%DeleteExtent()
    Do ##class(Lab6.CarModel).%DeleteExtent()
    Do ##class(Lab6.Customer).%DeleteExtent()
    Do ##class(Lab6.Employee).%DeleteExtent()
    Write "All demo data removed.", !
    Quit $$$OK
}

ClassMethod Test() As %Status
{
    Set ^UnitTestRoot = "/tmp/tests"
    Do ##class(%File).CreateDirectoryChain("/tmp/tests/Lab6")
    Write ##class(%File).DirectoryExists("/tmp/tests/Lab6")
    Set test = "Lab6:Lab6.Tests.MainTest"
    Set quals = "/noload/norecursive/nodelete"
    Do ##class(%UnitTest.Manager).RunTest(test, quals)
    Quit $$$OK
}

ClassMethod Trigger()
{
    Set car = ##class(Lab6.Car).%New()
    Set car.Vin = "VIN123456789"
    Set car.LicensePlate = "TEST123"
    Set car.Status = "Available"
    Set car.Mileage = 1000
    Set car.DailyRate = 50
    Do car.%Save()
    
    Write "Initial mileage: ", car.Mileage, !
    
    Set car.Mileage = 900
    Set sc = car.%Save()
    If 'sc {
        Write "Trigger blocked mileage rollback as expected.", !
    } Else {
        Write "Error: Mileage rollback was allowed!", !
    }
    
    Set car.Mileage = 1200
    Set sc = car.%Save()
    If sc {
        Write "Mileage update allowed, new mileage: ", car.Mileage, !
    } Else {
        Write "Error: Mileage update blocked incorrectly!", !
    }
    
    Quit
}

}
