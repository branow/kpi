Class Lab7.Person Extends (%Persistent, %XML.Adaptor, %Library.Populate) [ Abstract ]
{

Property Name As %String(MAXLEN = 50, MINLEN = 2) [ Required ];

Property Email As %String(POPSPEC = "##class(Lab7.PopulateHelper).GetEmail()");

Property Address As Lab7.Address;

Property ContactMethods As array Of %String;

Method ValidateName() As %Boolean
{
    Quit ($Length(..name) >= 2) && ($Length(..name) <= 50)
}

Method OnPopulate() As %Status
{
    Do ..ContactMethods.Clear()
    Do ..ContactMethods.SetAt(##class(Lab7.PopulateHelper).GetPhoneNumber(), "Mobile")
    
    If $Random(2) {
        Do ..ContactMethods.SetAt(##class(Lab7.PopulateHelper).GetPhoneNumber(), "Work")
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
<DataLocation>^Lab7.PersonD</DataLocation>
<DefaultData>PersonDefaultData</DefaultData>
<IdLocation>^Lab7.PersonD</IdLocation>
<IndexLocation>^Lab7.PersonI</IndexLocation>
<StreamLocation>^Lab7.PersonS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
