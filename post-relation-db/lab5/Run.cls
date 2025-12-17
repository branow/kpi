Class Lab5.Run Extends %RegisteredObject
{

ClassMethod Setup(count As %Integer = 10) As %Status
{
    Do ##class(Lab5.CarModel).Populate(count)
    Write "Created ", count, " CarModels", !

    Do ##class(Lab5.Car).Populate(count)
    Write "Created ", count, " Cars", !

    Do ##class(Lab5.Customer).Populate(count)
    Write "Created ", count, " Customers", !

    Do ##class(Lab5.Employee).Populate(count)
    Write "Created ", count, " Employees", !

    Do ##class(Lab5.RentalContract).Populate(count)
    Write "Created ", count, " RentalContracts", !

    Write !, "Demo Data Setup Complete", !
    Quit $$$OK
}

ClassMethod Reset() As %Status
{
    Do ##class(Lab5.Payment).%DeleteExtent()
    Do ##class(Lab5.RentalContract).%DeleteExtent()
    Do ##class(Lab5.Car).%DeleteExtent()
    Do ##class(Lab5.CarModel).%DeleteExtent()
    Do ##class(Lab5.Customer).%DeleteExtent()
    Do ##class(Lab5.Employee).%DeleteExtent()
    Write "All demo data removed.", !
    Quit $$$OK
}

ClassMethod Test() As %Status
{
    Set ^UnitTestRoot = "/tmp/tests"
    Do ##class(%File).CreateDirectoryChain("/tmp/tests/Lab5")
    Write ##class(%File).DirectoryExists("/tmp/tests/Lab5")
    Set test = "Lab5:Lab5.Tests.MainTest"
    Set quals = "/noload/norecursive/nodelete"
    Do ##class(%UnitTest.Manager).RunTest(test, quals)
    Quit $$$OK
}

}
