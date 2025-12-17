Class Lab5.Employee Extends Lab5.Person
{

Property Position As %String(POPSPEC = "##class(Lab5.PopulateHelper).GetPosition()");

Property HireDate As %Date(POPSPEC = "Date()");

Method CreateCar(vin As %String, plate As %String, model As Lab5.CarModel, rate As %Decimal) As Lab5.Car
{
    Set car = ##class(Lab5.Car).%New()
    Set car.Vin = vin
    Set car.LicensePlate = plate
    Set car.Model = model
    Set car.DailyRate = rate
    Set car.Status = "Available"
    Do car.%Save()
    Quit car
}

Method InspectCar(car As Lab5.Car) As %Boolean
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
