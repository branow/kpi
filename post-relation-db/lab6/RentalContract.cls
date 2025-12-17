Class Lab6.RentalContract Extends (%Persistent, %Library.Populate)
{

Property StartDate As %Date(POPSPEC = "Date()") [ Required ];

Property EndDate As %Date(POPSPEC = "Date()") [ Required ];

Property AgreementText As %Stream.GlobalCharacter;

Property Car As Lab6.Car;

Relationship Customer As Lab6.Customer [ Cardinality = one, Inverse = RentalHistory ];

Relationship Payments As Lab6.Payment [ Cardinality = children, Inverse = RentalContract ];

Property Closed As %Boolean;

Property RentalDays As %Integer [ Calculated, SqlComputeCode = { Set {*RentalDays} = {EndDate} - {StartDate} + 1 }, SqlComputed ];

Property TotalPayment As %Decimal [ Calculated, SqlComputeCode = {
    Set {TotalPayment} = ##class(Lab6.RentalContract).%OpenId({%%ID}).TotalPaymentGet()
  }, SqlComputed ];

ClassMethod ReportContractsCursor()
{
    &sql(DECLARE ContractCursor CURSOR FOR 
         SELECT ID, Customer->Name, TotalPayment
         INTO :id,  :custName, :total
         FROM Lab6.RentalContract)
    
    &sql(OPEN ContractCursor)
    
    If SQLCODE { 
        Write "Error opening cursor (SQLCODE: ", SQLCODE, ")", ! 
        Quit 
    }
    
    For {
        &sql(FETCH ContractCursor)
        Quit:(SQLCODE '= 0)
        Write "Contract #", id, " | Customer: ", custName, " | Total: ", total, !
    }
    
    &sql(CLOSE ContractCursor)
}

Method OnPopulate() As %Status
{
    Set duration = $Random(14) + 1
    Set ..EndDate = ..StartDate + duration

    Do ..AgreementText.Write(##class(Lab6.PopulateHelper).GetAgreementText())
    Do ..Payments.Clear()
    
    Set pCount = $Random(3) + 1
    For i=1:1:pCount {
        Set pay = ##class(Lab6.Payment).%New()
        Set pay.Amount = 50 + $Random(450)
        Set pay.PaymentDate = ..StartDate + $Random(duration)
        Set pay.Method = ##class(Lab6.PopulateHelper).GetPaymentMethod()
        Set pay.Processed = $Random(2)
        Do ..Payments.Insert(pay)
    }

    Quit $$$OK
}

Method RentalDaysGet() As %Integer
{
    If (..StartDate="") || (..EndDate="") Quit 0
    Quit (..EndDate - ..StartDate) + 1
}

Method AddPayment(p As Lab6.Payment)
{
    Do ..Payments.Insert(p)
}

Method TotalPaymentGet() As %Decimal
{
    Set total = 0
    Set count = ..Payments.Count()
    If count = 0 Quit 0
    
    For i=1:1:count {
        Set pay = ..Payments.GetAt(i)
        If $IsObject(pay) {
            Set total = total + pay.Amount
        }
    }
    Quit total
}

Method CloseContract()
{
    Set ..Closed = 1
}

Method IsActive() As %Boolean
{
    Quit '..Closed
}

Storage Default
{
<Data name="RentalContractDefaultData">
<Value name="1">
<Value>%%CLASSNAME</Value>
</Value>
<Value name="2">
<Value>StartDate</Value>
</Value>
<Value name="3">
<Value>EndDate</Value>
</Value>
<Value name="4">
<Value>AgreementText</Value>
</Value>
<Value name="5">
<Value>Car</Value>
</Value>
<Value name="6">
<Value>Customer</Value>
</Value>
<Value name="7">
<Value>Closed</Value>
</Value>
</Data>
<DataLocation>^Lab6.RentalContractD</DataLocation>
<DefaultData>RentalContractDefaultData</DefaultData>
<IdLocation>^Lab6.RentalContractD</IdLocation>
<IndexLocation>^Lab6.RentalContractI</IndexLocation>
<StreamLocation>^Lab6.RentalContractS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
