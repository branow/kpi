Class Lab7.Payment Extends (%Persistent, %Library.Populate)
{

Property Amount As %Decimal(MINVAL = 0.0) [ Required ];

Property PaymentDate As %Date;

Property Method As %String;

Property Processed As %Boolean;

Relationship RentalContract As Lab7.RentalContract [ Cardinality = parent, Inverse = Payments ];

ClassMethod MarkAsProcessedSQL(paymentId As %String) As %Status
{
    &sql(UPDATE Lab7.Payment SET Processed = 1 WHERE ID = :paymentId)
    
    If SQLCODE = 0 {
        Write "Payment ", paymentId, " successfully updated (SQLCODE=0).", !
        Quit $$$OK
    } 
    ElseIf SQLCODE = 100 {
        Write "Error: Payment with ID ", paymentId, " not found (SQLCODE=100).", !
        Quit $$$ERROR($$$GeneralError, "Payment not found")
    } 
    Else {
        Write "SQL Error: ", SQLCODE, " - ", %msg, !
        Quit $$$ERROR($$$GeneralError, "SQL Error: " _ SQLCODE)
    }
}

Method Process() As %Boolean
{
    Set ..Processed = 1
    Quit 1
}

Method Refund() As %Boolean
{
    Set ..Processed = 0
    Quit 1
}

Storage Default
{
<Data name="PaymentDefaultData">
<Value name="1">
<Value>%%CLASSNAME</Value>
</Value>
<Value name="2">
<Value>Amount</Value>
</Value>
<Value name="3">
<Value>PaymentDate</Value>
</Value>
<Value name="4">
<Value>Method</Value>
</Value>
<Value name="5">
<Value>Processed</Value>
</Value>
</Data>
<DataLocation>{%%PARENT}("Payments")</DataLocation>
<DefaultData>PaymentDefaultData</DefaultData>
<IdLocation>^Lab7.RentalContractC("Payments")</IdLocation>
<IndexLocation>^Lab7.PaymentI</IndexLocation>
<StreamLocation>^Lab7.PaymentS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
