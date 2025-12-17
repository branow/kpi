Class Lab7.PopulateHelper
{

ClassMethod GetCarBrand() As %String
{
        Set data = $ListBuild("Toyota", "BMW", "Audi", "Mercedes", "Honda", "Ford", "Tesla", "Chevrolet", "Kia", "Volkswagen", "Nissan", "Hyundai")
        Quit $List(data, $Random($ListLength(data)) + 1)
}

ClassMethod GetCarModel() As %String
{
        Set data = $ListBuild("Camry", "Corolla", "X5", "A4", "Civic", "Mustang", "Model S", "Focus", "Rio", "Passat", "Golf", "Tucson", "Sportage")
        Quit $List(data, $Random($ListLength(data)) + 1)
}

ClassMethod GetCarFeature() As %String
{
        Set data = $ListBuild("GPS", "Bluetooth", "Leather Seats", "Sunroof", "Heated Seats", "Navigation", "Backup Camera", "Autopilot", "Sport Mode", "Apple CarPlay", "Android Auto", "Parking Sensors", "Cruise Control")
        Quit $List(data, $Random($ListLength(data)) + 1)
}

ClassMethod GetVIN() As %String
{
            Set chars = "0123456789ABCDEFGHJKLMNPRSTUVWXYZ"
            Set vin = ""
            For i=1:1:17 { 
                Set vin = vin _ $Extract(chars, $Random($Length(chars)) + 1) 
            }
            Quit vin
}

ClassMethod GetLicensePlate() As %String
{
            Set letters = "ABCEHIKMOPTX" 
            Set plate = ""
            For i=1:1:2 { Set plate = plate _ $Extract(letters, $Random($Length(letters))+1) }
            For i=1:1:4 { Set plate = plate _ $Random(10) }
            For i=1:1:2 { Set plate = plate _ $Extract(letters, $Random($Length(letters))+1) }
            Quit plate
}

ClassMethod GetCarStatus() As %String
{
            Set r = $Random(100)
            If r < 70 Quit "Available"    
            If r < 90 Quit "Rented"       
            Quit "Maintenance"
}

ClassMethod GetDriverLicense() As %String
{
        Set letters = "ABCEHIKMOPTX"
        Set result = ""
        For i=1:1:3 { Set result = result _ $Extract(letters, $Random($Length(letters))+1) }
        For i=1:1:6 { Set result = result _ $Random(10) }
        Quit result
}

ClassMethod GetPosition() As %String
{
        Set jobs = $ListBuild("Manager", "Sales Agent", "Mechanic", "Washer", "Accountant", "Security", "Intern", "Director")
        Quit $List(jobs, $Random($ListLength(jobs)) + 1)
}

ClassMethod GetPhoneNumber() As %String
{
        Quit "+380" _ (50 + $Random(50)) _ (100000 + $Random(900000))
}

ClassMethod GetEmail() As %String
{
        Set names = $ListBuild("alex", "maria", "ivan", "olga", "sergiy", "kate", "dmytro", "iryna", "admin", "info")
        Set domains = $ListBuild("gmail.com", "ukr.net", "yahoo.com", "outlook.com", "company.com", "rental.ua")
        
        Set name = $List(names, $Random($ListLength(names)) + 1)
        Set domain = $List(domains, $Random($ListLength(domains)) + 1)
        
        Quit name _ $Random(1000) _ "@" _ domain
}

ClassMethod GetPaymentMethod() As %String
{
        Set methods = $ListBuild("Credit Card", "Cash", "Debit Card", "PayPal", "Bank Transfer", "Apple Pay", "Google Pay")
        Quit $List(methods, $Random($ListLength(methods)) + 1)
}

ClassMethod GetAgreementText() As %String
{
        Quit "Standard rental agreement terms apply." _
             "Insurance ID: " _ ..GetVIN() _ ". Authorized by: " _ ..GetEmail()
}

}
