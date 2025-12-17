Class Lab7.Employee Extends Lab7.Person
{

Property Position As %String(POPSPEC = "##class(Lab7.PopulateHelper).GetPosition()");

Property HireDate As %Date(POPSPEC = "Date()");

ClassMethod FindEmployeesDynamic(namePart As %String)
{
    Set statement = ##class(%SQL.Statement).%New()
    Set sql = "SELECT Name, Position, HireDate FROM Lab6.Employee WHERE Name %STARTSWITH ?"
    Set status = statement.%Prepare(sql)
    
    If $$$ISERR(status) {
        Write "SQL preparation error: ", $System.Status.GetErrorText(status), !
        Quit
    }
    
    Set rset = statement.%Execute(namePart)
    
    Write "Query type: ", rset.%GetMetadata().statementType, " (SELECT)", !
    Write "Search results: (" _ namePart _ "...):", !
    
    Set count = 0
    While rset.%Next() {
        Set count = count + 1
        Write count, ". ", rset.%Get("Name"), " — ", rset.%Get("Position"), 
              " (Date: ", rset.%Get("HireDate"), ")", !
    }
    
    If count = 0 Write "  Nothing found.", !
}

Method CreateCar(vin As %String, plate As %String, model As Lab7.CarModel, rate As %Decimal) As Lab7.Car
{
    Set car = ##class(Lab7.Car).%New()
    Set car.Vin = vin
    Set car.LicensePlate = plate
    Set car.Model = model
    Set car.DailyRate = rate
    Set car.Status = "Available"
    Do car.%Save()
    Quit car
}

Method InspectCar(car As Lab7.Car) As %Boolean
{
    Quit 1
}

Storage Default
{
<Data name="EmployeeDefaultData">
<Subscript>"Employee"</Subscript>
<Value name="1">
<Value>Position</Value>
</Value>
<Value name="2">
<Value>HireDate</Value>
</Value>
</Data>
<DefaultData>EmployeeDefaultData</DefaultData>
<Type>%Storage.Persistent</Type>
}

}
