Class User.GlobalDemo
{

ClassMethod Run()
{

    kill ^RENTALS

    // Car details list
    set ^RENTALS("John Smith","2025-10-12",3001) = $lb("Toyota","Corolla","Sedan","Automatic",7,350,"GPS","Child Seat")
    set ^RENTALS("John Smith","2025-10-20",3005) = $lb("Honda","CR-V","SUV","Automatic",3,280,"GPS")
    set ^RENTALS("Mary Johnson","2025-10-15",3003) = $lb("Honda","Civic","Sedan","Manual",5,300)
    set ^RENTALS("Alice Brown","2025-10-12",4001) = $lb("Ford","Escape","SUV","Automatic",3,450)
    set ^RENTALS("Alice Brown","2025-10-18",4002) = $lb("Ford","Fiesta","Hatchback","Manual",4,200)
    set ^RENTALS("Bob Martin","2025-10-16",5001) = $lb("BMW","X3","SUV","Automatic",4,480,"GPS")
    set ^RENTALS("Emma White","2025-10-17",6002) = $lb("Audi","A4","Sedan","Manual",2,220)
    set ^RENTALS("Tom Green","2025-10-18",7003) = $lb("Tesla","Model 3","Sedan","Automatic",3,600,"Child Seat")
    set ^RENTALS("Tom Green","2025-10-22",7004) = $lb("Tesla","Model X","SUV","Automatic",2,800,"GPS")
    set ^RENTALS("Sarah Lee","2025-10-19",8001) = $lb("Nissan","Altima","Sedan","Automatic",5,320,"GPS")
    set ^RENTALS("Michael King","2025-10-21",9001) = $lb("Chevrolet","Impala","Sedan","Automatic",3,270,"GPS","Child Seat")
    set ^RENTALS("Olivia White","2025-10-20",10001) = $lb("Kia","Sorento","SUV","Automatic",6,400,"GPS")

    // Payments
    set ^RENTALS("John Smith","2025-10-12",3001,"payment") = $lb("Credit Card",350,0)
    set ^RENTALS("John Smith","2025-10-20",3005,"payment") = $lb("Cash",150,130)
    set ^RENTALS("Mary Johnson","2025-10-15",3003,"payment") = $lb("Cash",150,150)
    set ^RENTALS("Alice Brown","2025-10-12",4001,"payment") = $lb("Credit Card",450,0)
    set ^RENTALS("Alice Brown","2025-10-18",4002,"payment") = $lb("Cash",100,100)
    set ^RENTALS("Bob Martin","2025-10-16",5001,"payment") = $lb("Credit Card",480,0)
    set ^RENTALS("Emma White","2025-10-17",6002,"payment") = $lb("Cash",100,120)
    set ^RENTALS("Tom Green","2025-10-18",7003,"payment") = $lb("Credit Card",600,0)
    set ^RENTALS("Tom Green","2025-10-22",7004,"payment") = $lb("Credit Card",800,0)
    set ^RENTALS("Sarah Lee","2025-10-19",8001,"payment") = $lb("Credit Card",320,0)
    set ^RENTALS("Michael King","2025-10-21",9001,"payment") = $lb("Cash",100,170)
    set ^RENTALS("Olivia White","2025-10-20",10001,"payment") = $lb("Credit Card",400,0)

    /// --- SEARCH BY CAR MODEL ---

    write !,"=== Search Rentals by Car Model ===",!
    read "Enter car model to search: ", model, !

    set x=$name(^RENTALS)
    for {
        set x=$query(@x)
        if x="" quit
        if $qlength(x)=3,$listfind($get(@x), model)>0 {
            write "Customer: ", $qsubscript(x,1), ", Date: ", $qsubscript(x,2), ", CarID: ", $qsubscript(x,3),!
            write "Car Details: ", $listtostring(@x,", "),!,!
        }
    }

    /// --- FLEXIBLE SEARCH BY CUSTOMER NAME AND DATE ---

    write !!,"=== Search by Customer Name and Date ===",!!

    read "Enter customer name (first or last, leave blank to skip): ", searchName, !!
    read "Enter date (YYYY, YYYY-MM, or YYYY-MM-DD, leave blank to skip): ", searchDate, !!

    set x=$name(^RENTALS)
    for {
        set x=$query(@x)
        if x="" quit
        if $qlength(x)=3 {
            set custName=$qsubscript(x,1)
            set rentalDate=$qsubscript(x,2)

            set nameMatch=0
            if searchName="" set nameMatch=1
            else  if $find(custName,searchName) set nameMatch=1

            set dateMatch=0
            if searchDate="" set dateMatch=1
            else  if $find(rentalDate,searchDate) set dateMatch=1

            if nameMatch,dateMatch {
                write !,"Customer: ", custName, ", Date: ", rentalDate, ", CarID: ", $qsubscript(x,3),!
                write "Car Details: ", $listtostring(@x,", "),!
                if $data(^RENTALS(custName,rentalDate,$qsubscript(x,3),"payment")) {
                    write "Payment: ", $listtostring($get(^RENTALS(custName,rentalDate,$qsubscript(x,3),"payment")),", "),!
                }
            }
        }
    }
}

}
