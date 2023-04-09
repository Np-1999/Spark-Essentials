package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App{
  // Values and variables
  val aBoolean: Boolean = false

  // Expressions

  val anIfExp = if (2>3) "bigger" else "smaller"

  // Instructions vs Expressions
  // Instruction = Imperative
  // Expressiong = Functional language

  // Functions
  def myFunction(x: Int) = 42

  // oop

  class Animal
  class Cat extends Animal
  trait carnivore {
    def eat(animal: Animal): Unit
  }
  class Crocodile extends Animal with carnivore{
    override def eat(animal: Animal): Unit = println("Crunch")
  }

  // Singleton pattern
  object MySingleton
  // Companions
  object carnivore

  // generics
  trait MyList[A]

  // Method notation
  val x = 1 + 2
  val y = 1.+(2)


  // Functional Programming
  val incrementer: Int =>Int  = (x:Int) => x + 1
  val incremented = incrementer(42)

  // map, flatmap, filter
  val procesedList = List(1,2,3).map(incrementer)

  // Pattern matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "One"
    case 2 => "Second"
    case _ => "unknown"
  }
  // try-catch
  try{
    throw  new NullPointerException()
  }catch{
    case e: NullPointerException => "Error"
    case _ => "Something else"
  }

  // Future
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    // Some expensive computation, runs on another thread
    42
  }
  aFuture.onComplete{
    case  Success(meaningOfLife) => println(s"I've found $meaningOfLife")
    case Failure(ex) => println(s"I have failed $ex")
  }

  // Partial functions
  val aPartialFunction: PartialFunction[Int, Int] ={
    case 1 => 42
    case 2 => 12
    case _ => 54
  }

  // Implicits
  // auto injection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x+43
  implicit val implicitInt = 67
  val implicitCall = methodWithImplicitArgument

  // Implicit conversions - implicit defs
  case class Person(name: String){
    def greet= println(s"Hi, my name is $name")
  }
  implicit def fromStringtoPerson(name: String) = Person(name)
  "Bob".greet

  // implicit conversion
  implicit class Dog(name: String) {
    def bark = println("BARK!!!")
  }
  "Lassie".bark
  /*
    - local scope
    - imported scope
    - companion objects of the types involved in the method call
  */


}
