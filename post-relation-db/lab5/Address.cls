Class Lab5.Address Extends (%SerialObject, %Library.Populate)
{

Property Street As %String;

Property City As %String;

Property ZipCode As %String;

Storage Default
{
<Data name="AddressState">
<Value name="1">
<Value>Street</Value>
</Value>
<Value name="2">
<Value>City</Value>
</Value>
<Value name="3">
<Value>ZipCode</Value>
</Value>
</Data>
<State>AddressState</State>
<StreamLocation>^Lab5.AddressS</StreamLocation>
<Type>%Storage.Serial</Type>
}

}
