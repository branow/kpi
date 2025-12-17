Class Lab7.RentalContract Extends (%Persistent, %Library.Populate)
{

Property StartDate As %Date(POPSPEC = "Date()") [ Required ];

Property EndDate As %Date(POPSPEC = "Date()") [ Required ];

Property AgreementText As %Stream.GlobalCharacter;

Property Car As Lab7.Car;

Relationship Customer As Lab7.Customer [ Cardinality = one, Inverse = RentalHistory ];

Relationship Payments As Lab7.Payment [ Cardinality = children, Inverse = RentalContract ];

Property Closed As %Boolean;

Property RentalDays As %Integer [ Calculated, SqlComputeCode = { Set {*RentalDays} = {EndDate} - {StartDate} + 1 }, SqlComputed ];

Property TotalPayment As %Decimal [ Calculated, SqlComputeCode = {
    Set {TotalPayment} = ##class(Lab7.RentalContract).%OpenId({%%ID}).TotalPaymentGet()
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
    // 1. Логіка Дат: Гарантуємо, що EndDate > StartDate
    // Генеруємо тривалість від 1 до 14 днів
    Set duration = $Random(14) + 1
    Set ..EndDate = ..StartDate + duration

    // 2. Заповнюємо текст угоди (якщо stream)
    Do ..AgreementText.Write(##class(Lab7.PopulateHelper).GetAgreementText())

    // 3. Якщо Customer не заповнився автоматично (через особливості Relationship), 
    // виберемо випадкового вручну. Але зазвичай для One-side це працює.
    // Якщо поле пусте, розкоментуйте:
    // If ..Customer="" Set ..Customer = ##class(Lab7.Customer).%OpenId($Random(20)+1) 

    // 4. ГЕНЕРАЦІЯ ПЛАТЕЖІВ (Child Objects)
    // Очищаємо старі платежі, якщо є
    Do ..Payments.Clear()
    
    // Створюємо від 1 до 3 платежів
    Set pCount = $Random(3) + 1
    For i=1:1:pCount {
        Set pay = ##class(Lab7.Payment).%New()
        
        // Сума від 50 до 500
        Set pay.Amount = 50 + $Random(450)
        
        // Дата платежу - десь між початком і кінцем оренди
        Set pay.PaymentDate = ..StartDate + $Random(duration)
        
        Set pay.Method = ##class(Lab7.PopulateHelper).GetPaymentMethod()
        Set pay.Processed = $Random(2) // 0 або 1
        
        // Прив'язуємо платіж до цього контракту
        Do ..Payments.Insert(pay)
    }

    Quit $$$OK
}

Method RentalDaysGet() As %Integer
{
    If (..StartDate="") || (..EndDate="") Quit 0
    Quit (..EndDate - ..StartDate) + 1
}

Method AddPayment(p As Lab7.Payment)
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
<DataLocation>^Lab7.RentalContractD</DataLocation>
<DefaultData>RentalContractDefaultData</DefaultData>
<IdLocation>^Lab7.RentalContractD</IdLocation>
<IndexLocation>^Lab7.RentalContractI</IndexLocation>
<StreamLocation>^Lab7.RentalContractS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
