Class Lab7.Run Extends %RegisteredObject
{

ClassMethod Setup(count As %Integer = 10) As %Status
{
    Do ##class(Lab7.CarModel).Populate(count)
    Write "Created ", count, " CarModels", !

    Do ##class(Lab7.Car).Populate(count)
    Write "Created ", count, " Cars", !

    Do ##class(Lab7.Customer).Populate(count)
    Write "Created ", count, " Customers", !

    Do ##class(Lab7.Employee).Populate(count)
    Write "Created ", count, " Employees", !

    Do ##class(Lab7.RentalContract).Populate(count)
    Write "Created ", count, " RentalContracts", !

    Do ##class(Lab7.Payment).Populate(count)
    Write "Created ", count, " Payments", !

    Write !, "Demo Data Setup Complete", !
    Quit $$$OK
}

ClassMethod Reset() As %Status
{
    Do ##class(Lab7.Payment).%DeleteExtent()
    Do ##class(Lab7.RentalContract).%DeleteExtent()
    Do ##class(Lab7.Car).%DeleteExtent()
    Do ##class(Lab7.CarModel).%DeleteExtent()
    Do ##class(Lab7.Customer).%DeleteExtent()
    Do ##class(Lab7.Employee).%DeleteExtent()
    Write "All demo data removed.", !
    Quit $$$OK
}

ClassMethod Test() As %Status
{
    Set ^UnitTestRoot = "/tmp/tests"
    Do ##class(%File).CreateDirectoryChain("/tmp/tests/Lab7")
    Write ##class(%File).DirectoryExists("/tmp/tests/Lab7")
    Set test = "Lab7:Lab7.Tests.MainTest"
    Set quals = "/noload/norecursive/nodelete"
    Do ##class(%UnitTest.Manager).RunTest(test, quals)
    Quit $$$OK
}

}
