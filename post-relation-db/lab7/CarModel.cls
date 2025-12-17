Class Lab7.CarModel Extends (%Persistent, %Library.Populate, %JSON.Adaptor, %XML.Adaptor)
{

Property Brand As %String(POPSPEC = "##class(Lab7.PopulateHelper).GetCarBrand()");

Property ModelName As %String(POPSPEC = "##class(Lab7.PopulateHelper).GetCarModel()");

Property Year As %Integer(MAXVAL = 2025, MINVAL = 2000);

Property Features As list Of %String;

Query FindByYearRange(MinYear As %Integer, MaxYear As %Integer) As %Query(ROWSPEC = "ID:%String,Brand:%String,ModelName:%String,Year:%Integer") [ SqlProc ]
{
}

ClassMethod FindByYearRangeExecute(ByRef qHandle As %Binary, MinYear As %Integer, MaxYear As %Integer) As %Status
{
    Set qHandle("CurrentID") = ""   
    Set qHandle("Min") = MinYear    
    Set qHandle("Max") = MaxYear    
    
    Quit $$$OK
}

ClassMethod FindByYearRangeFetch(ByRef qHandle As %Binary, ByRef Row As %List, ByRef AtEnd As %Integer = 0) As %Status
{
    Set Row = ""
    Set AtEnd = 0
    
    Set id = qHandle("CurrentID")
    Set min = qHandle("Min")
    Set max = qHandle("Max")
    
    For {
        Set id = $ORDER(^Lab7.CarModelD(id))
        
        If id = "" {
            Set AtEnd = 1
            Quit
        }
        
        Set data = $Get(^Lab7.CarModelD(id))
        Set year = $List(data, 4)
        
        If (year >= min) && (year <= max) {
            Set brand = $List(data, 2)
            Set model = $List(data, 3)
            Set Row = $ListBuild(id, brand, model, year)
            Set qHandle("CurrentID") = id
            Quit
        }
    }
    
    Quit $$$OK
}

ClassMethod FindByYearRangeClose(ByRef qHandle As %Binary) As %Status
{
    Kill qHandle
    Quit $$$OK
}

Method OnPopulate() As %Status
{
    Do ..Features.Clear()
    
    Set count = $Random(5) + 1
    
    For i=1:1:count {
        Set feature = ##class(Lab7.PopulateHelper).GetCarFeature()
        
        If ..Features.Find(feature) = "" {
            Do ..Features.Insert(feature)
        }
    }
    
    Quit $$$OK
}

Method AddFeature(f As %String)
{
    Do ..Features.Insert(f)
}

Method RemoveFeature(f As %String)
{
    Set i = ..Features.Find(f)
    If i>0 Do ..Features.RemoveAt(i)
}

Storage Default
{
<Data name="CarModelDefaultData">
<Value name="1">
<Value>%%CLASSNAME</Value>
</Value>
<Value name="2">
<Value>Brand</Value>
</Value>
<Value name="3">
<Value>ModelName</Value>
</Value>
<Value name="4">
<Value>Year</Value>
</Value>
<Value name="5">
<Value>Features</Value>
</Value>
</Data>
<DataLocation>^Lab7.CarModelD</DataLocation>
<DefaultData>CarModelDefaultData</DefaultData>
<IdLocation>^Lab7.CarModelD</IdLocation>
<IndexLocation>^Lab7.CarModelI</IndexLocation>
<StreamLocation>^Lab7.CarModelS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
