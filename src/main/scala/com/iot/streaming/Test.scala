package com.iot.streaming
case class Person (firstName: String, lastName: String)
object Test {
  def justexecute(per: Person) : Person = {
    per.copy(lastName = "Wells")
    
  }
   def main(args:Array[String]){
     println("hello")
     val emily1 = Person("Emily", "Maness")

     //println(justexecute(emily1).lastName)
     
     val a = Seq("1","2","3")
     //println(a.filter { !_.equals("1") })
     val i = 123
     val i1 = Integer.toString(i)
     println(i1.substring(i1.length()-1, i1.length()))
   
     
   }
}