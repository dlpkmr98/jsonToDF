package com.org.dilip.spark

import scala.util.Either

object ScalaEx extends App {
  
  
  def init[A,B](f1:(A,B) => A) = (f2:(A,B) =>A) => (a:A) => (b:B) => (c:B) => f2(f1(a,b),c)
  
  
  
  def sum(a:Int,b:Int) = a+b
  def sumdouble(a:Double,b:Double) = a+b
  def mul(a:Int,b:Int) = a*b
 
  
  println(init(sum)(mul)(1)(2)(3))
  
  
  
  
  
  
  
  
}
