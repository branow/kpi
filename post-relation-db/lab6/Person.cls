Class Lab6.Person Extends (%Persistent, %XML.Adaptor, %Library.Populate) [ Abstract ]
{

Property Name As %String(MAXLEN = 50, MINLEN = 2) [ Required ];

Property Email As %String(POPSPEC = "##class(Lab6.PopulateHelper).GetEmail()");

Property Address As Lab6.Address;

Property ContactMethods As array Of %String;

Method ValidateName() As %Boolean
{
    Quit ($Length(..name) >= 2) && ($Length(..name) <= 50)
}

Method OnPopulate() As %Status
{
    Do ..ContactMethods.Clear()
    Do ..ContactMethods.SetAt(##class(Lab6.PopulateHelper).GetPhoneNumber(), "Mobile")
    
    If $Random(2) {
        Do ..ContactMethods.SetAt(##class(Lab6.PopulateHelper).GetPhoneNumber(), "Work")
    }
    
    Quit $$$OK
}

Storage Default
{
<Data name="ContactMethods">
<Attribute>ContactMethods</Attribute>
<Structure>subnode</Structure>
<Subscript>"ContactMethods"</Subscript>
</Data>
<Data name="PersonDefaultData">
<Value name="1">
<Value>%%CLASSNAME</Value>
</Value>
<Value name="2">
<Value>Name</Value>
</Value>
<Value name="3">
<Value>Email</Value>
</Value>
<Value name="4">
<Value>Address</Value>
</Value>
</Data>
<DataLocation>^Lab6.PersonD</DataLocation>
<DefaultData>PersonDefaultData</DefaultData>
<IdLocation>^Lab6.PersonD</IdLocation>
<IndexLocation>^Lab6.PersonI</IndexLocation>
<StreamLocation>^Lab6.PersonS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
