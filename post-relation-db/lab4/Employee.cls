Class Lab4.Employee Extends Lab4.Person
{

Property EmployeeId As %String;

Index EmpIdIndex On EmployeeId [ Unique ];

Property Position As %String;

Property HireDate As %Date;

Method CreateCar(vin As %String, plate As %String, model As Lab4.CarModel, rate As %Decimal) As Lab4.Car
{
    Set car = ##class(Lab4.Car).%New()
    Set car.Vin = vin
    Set car.LicensePlate = plate
    Set car.Model = model
    Set car.DailyRate = rate
    Set car.Status = "Available"
    Do car.%Save()
    Quit car
}

Method InspectCar(car As Lab4.Car) As %Boolean
{
    Quit 1
}

Storage Default
{
<Data name="EmployeeDefaultData">
<Subscript>"Employee"</Subscript>
<Value name="1">
<Value>employeeId</Value>
</Value>
<Value name="2">
<Value>position</Value>
</Value>
<Value name="3">
<Value>hireDate</Value>
</Value>
<Value name="4">
<Value>EmployeeId</Value>
</Value>
<Value name="5">
<Value>Position</Value>
</Value>
<Value name="6">
<Value>HireDate</Value>
</Value>
</Data>
<DefaultData>EmployeeDefaultData</DefaultData>
<Type>%Storage.Persistent</Type>
}

}
