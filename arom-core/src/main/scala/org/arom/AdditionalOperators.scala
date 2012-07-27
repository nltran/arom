package org.arom
import org.arom._
import org.arom.QuicksortMergesort
import scala.collection.mutable.ArrayBuffer
import org.apache.pig.data.Tuple
import scala.collection.JavaConversions._
import org.apache.hadoop.io.Writable
import scala.collection.mutable.{Buffer, ArrayBuffer}
import org.apache.pig.data.BinSedesTuple
import akka.util.HashCode

object Factories {
  lazy val tuple = new org.apache.pig.data.BinSedesTupleFactory
  lazy val bag = org.apache.pig.data.BagFactory.getInstance
}

class WritableDataOps[T <: Writable] extends Data.DataOps[T] {
     import sbinary._
     import sbinary.Operations._
     import sbinary.DefaultProtocol._
     def memorySize(t) = 8
     implicit object binFormat extends Format[Buffer[T]] {
         def reads(in : Input) = {
             def br: Int = 0xff & read[Byte](in).asInstanceOf[Int]
             val stream = new java.io.InputStream {
                 def read = br
             }
             //val bytes = read[Array[Byte]](in)
             val in2 = new java.io.DataInputStream(stream)
             val numitems = in2.readInt
             val result = new ArrayBuffer[T](numitems)
             1 to numitems foreach { _ =>
                 val tuple = (Factories.tuple newTuple).asInstanceOf[T]
                 tuple readFields in2
                 result += tuple
             }
             in2 close;
             result
         }

         def writes(out: Output, value: Buffer[T]) = {
             def bw(b: Int) = write[Byte](out, b.toByte)
             val stream = new java.io.OutputStream {
                 def write(b: Int) = bw(b)
             }
             //val bytes = new java.io.ByteArrayOutputStream(value.size* memorySize(value.head))
             val out2 = new java.io.DataOutputStream(stream)
             out2 writeInt value.size
             value foreach {_.asInstanceOf[BinSedesTuple] write out2}
             out2 close;
             //write[Array[Byte]](out, bytes toByteArray)
         }
     }
}

object BinSedesTupleDataOps extends WritableDataOps[Tuple] {
     import sbinary._
     import sbinary.Operations._
     import sbinary.DefaultProtocol._
     override def memorySize(t) = t.getMemorySize.toInt
}

@serializable object PigDataRegistration extends (() => Unit) {
	def apply = Data register BinSedesTupleDataOps
}


//object StringArrayDataOps extends Data.DataOps[Array[String]] {
// import sbinary._
// import sbinary.Operations._
// import sbinary.DefaultProtocol._
// def memorySize(a: Array[String]) = a map {_.size * 2 + 38} sum
// implicit object binFormat extends Format[Buffer[Array[String]]] {
//   def reads(in: Input) = {
//     val nbitems = read[Int](in)
//     val result = new ArrayBuffer[Array[String]](nbitems)
//     1 to nbitems foreach {_ => result += read[Array[Array[Byte]]](in) map {new String(_)}}
//     result
//   }
//   def writes(out: Output, buf: Buffer[Array[String]]) = {
//     write[Int](out, buf.size)
//     buf foreach {b => write[Array[Array[Byte]]](out, b map {_ getBytes})}
//   }
// }
//}


 

//@serializable object StringArrayRegistration extends (() => Unit) {
//	def apply = Data register StringArrayDataOps
//}


class FormatUserWebsites[T](implicit m: Manifest[T]) extends ScalarOperator {
  def process(runtime) = {
    case (_: Int, t: String) =>
      val value = t.split(",")
      val website = value(0)
      runtime emit (website.replace("http://", "").replace("https://", "").replace("www.", "") + "," + value(1) + "," + value(2))
    case (_: Int, t: Tuple) =>
      var t1 = t.get(1).asInstanceOf[Tuple]
      t1.set(0, t1.get(0).toString().replace("http://", "").replace("https://", "").replace("www.", ""))
      runtime emit (t)
  }
  def copy = new FormatUserWebsites
}

class MergeJoin(implicit ord: RepJoinOrdering) extends QuicksortMergesort[Tuple](false) {
  override def copy = new MergeJoin
  private def doJoin(l: Tuple, r: Tuple): Tuple = {
    val fields = new java.util.ArrayList[AnyRef](l.getAll.size + r.getAll.size)
    fields addAll l.getAll
    fields addAll r.getAll
    Factories.tuple newTupleNoCopy fields
  }
  override def finish(rt) = if (!data.isEmpty) {
    val iter = resultIterator
    val tpl = new ArrayBuffer[Tuple]
    var oldkey: Any = null

    while (iter hasNext) {
      val next = iter.next;
      val tuple = (next get 1).asInstanceOf[Tuple]
      val key = tuple get 0
      val tag = (next get 0).asInstanceOf[Int]

      if (oldkey != key)
        tpl clear;
      if (tag == 0)
        tpl += tuple
      else {
        
        tpl foreach {t => rt.emit[Tuple](doJoin(t,tuple))}
//        tpl foreach { t => rt.emit[Tuple](doJoin(t, tuple).get(1).toString.hashCode.abs % rt.nOutputs, Factories.tuple newTupleNoCopy List(doJoin(t, tuple).get(1), doJoin(t, tuple).get(0), doJoin(t, tuple).get(4))) }

      }

      oldkey = key
    }
    clear
  }
}

class QuicksortGroup(implicit ord: GroupOrdering) extends QuicksortMergesort[Tuple](true) {
  override def copy = new QuicksortGroup
  override def finish(rt) = if (!data.isEmpty) {
    val iter = resultIterator
    var tpl = new java.util.ArrayList[String]
    var currentkey: String = null
    var key: String = null

    while (iter hasNext) {
      val next = iter next
      val value = (next get 1).toString()
      key = (next get 0).toString()

      if (currentkey == null)
        currentkey = new String(key)
      if (currentkey.equals(key))
        tpl.add(value)
      else {
        tpl.add(0, currentkey)
        rt.emit[Tuple](Factories.tuple newTupleNoCopy (List(currentkey, tpl.size()-1)))
//        rt.emit[Tuple](Factories.tuple newTupleNoCopy (tpl))

        //        rt.emit[Tuple](Factories.tuple newTupleNoCopy (tpl))
        currentkey = new String(key)
        tpl = new java.util.ArrayList[String]
        tpl.add(value)

      }
    }
    tpl.add(0, key)
//        rt.emit[Tuple](Factories.tuple newTupleNoCopy (tpl))
    rt.emit[Tuple](Factories.tuple newTupleNoCopy (List(currentkey, tpl.size()-1)))
  }
}

//class RepJoinOrdering extends Ordering[Tuple] {
//  def compare(l: Tuple, r: Tuple) = {
//    val lkey = (l get 1).asInstanceOf[Comparable[Any]]
//    val rkey = (r get 1).asInstanceOf[Comparable[Any]]
//    val ltag = (l get 0).asInstanceOf[Int]
//    val rtag = (r get 0).asInstanceOf[Int]
//    val keydiff = lkey compareTo rkey
//    if (keydiff == 0) ltag - rtag else keydiff
//  }
//}
class RepJoinOrdering extends Ordering[Tuple] {
  def compare(l: Tuple, r: Tuple) = {
    val lkey = (l get 1).asInstanceOf[Tuple].get(0).asInstanceOf[Comparable[Any]]
    val rkey = (r get 1).asInstanceOf[Tuple].get(0).asInstanceOf[Comparable[Any]]
    val ltag = (l get 0).asInstanceOf[Int]
    val rtag = (r get 0).asInstanceOf[Int]
    val keydiff = lkey compareTo rkey
    if (keydiff == 0) ltag - rtag else keydiff
  }
}

class Probe(n: Int) extends ScalarOperator {
  var total:Long = 0;
  var size:Long=0;
  def process(rt) = {
    case(i: Int, t:Array[String]) => 
      if (n !=0){
        if(total < n){
    	  println("received on input %d: %s" format (i, t.toSeq.toString()))
    	  total += 1
        }
      }
      else if (n == 0){
        println("received on input %d: %s" format (i, t.toSeq.toString()))
      } 
      
      size +=t.size
//      if (rt.nOutputs >= i)
//      if (total < n)
//      	rt.emit(i,t)
  }
  override def finish(rt) = {
    println("traffic: %d elements, totalling %d bytes" format (total,size))
  }
  def copy = new Probe(n)
}

class GroupOrdering extends Ordering[Tuple] {
  def compare(t1: Tuple, t2: Tuple) = {
    t1.get(0).toString().compareTo(t2.get(0).toString())
  }
}

class TagN(n: Int) extends ScalarOperator {
  def process(rt) = {
    case (_: Int, t: Tuple) =>
//      var outputBuffer = new java.util.ArrayList[String]
//      outputBuffer.add(0,n.toString())
      rt.emit[Tuple](Factories.tuple newTupleNoCopy List(n, t))
  }
  def copy = new TagN(n)
}

class PartitionTupleOnKey[T](implicit m: Manifest[T]) extends ScalarOperator {
	def process(runtime) = { case(_: Int, t: Tuple) =>
		runtime emit (t.get(1).asInstanceOf[Tuple].get(0).toString().hashCode().abs % runtime.nOutputs, t)
	}
	def copy = new PartitionTupleOnKey
}


class SimpleGroup extends ScalarOperator {
  var currentKey: String = null
  var outputBuffer = new java.util.ArrayList[String]
  def process(rt) = {
    case (_: Int, t: Tuple) =>
      val tupleKey = t.get(0).toString
      if (currentKey == null)
        currentKey = new String(tupleKey)
      if (tupleKey.equals(currentKey))
        outputBuffer add (t.get(1).toString())
      else {
        outputBuffer.add(0, currentKey)
        rt.emit[Tuple](Factories.tuple newTupleNoCopy (outputBuffer))
        currentKey = new String(tupleKey)
        outputBuffer = new java.util.ArrayList[String]
        outputBuffer add (t.get(1).toString)
      }
  }
  override def finish(rt) = {
    outputBuffer add (0, currentKey)
    rt.emit[Tuple](Factories.tuple newTupleNoCopy (outputBuffer))
  }
  def copy = new SimpleGroup
}

class ReorderByUser extends ScalarOperator {
  def process(rt) = {
    case (_: Int, t: Tuple) =>
      var output = new java.util.ArrayList[Object]
      output.add(t.get(1))
      output.add(t.get(0))
      output.add(t.get(4))
      rt.emit[Tuple](Factories.tuple newTupleNoCopy (output))
  }
  def copy = new ReorderByUser
}

class CountFields extends ScalarOperator {
  def process(rt) = {
    case (_: Int, t: Tuple) =>
      rt.emit[Tuple](Factories.tuple newTupleNoCopy List(t get 0, t.size - 1))
  }
  def copy = new CountFields
}

class Wait extends ScalarOperator {
  def process(rt) = { 
    case (_: Int, t: Tuple) =>
//      Thread.sleep(10000)
      rt.emit[Tuple](t)
  }
  def copy = new Wait
}

class RandomUserlogData() extends InputOperator {
  def copy = throw new Error("This shouldn't happen")
  private var _hasData = true
  def hasData = _hasData
  val entry1 = "text.com, usera, 201101021457"
  val entry2 = "texta.com, usera, 201101021458"
  val entry3 = "textb.com, userc, 201101021459"
  val entry4 = "textc.com, userd, 201101021500"

  def sendData(rt) = {
    rt.emit(0, text2Tuple(entry1))
    rt.emit(0, text2Tuple(entry2))
    rt.emit(0, text2Tuple(entry3))
    rt.emit(0, text2Tuple(entry4))

    _hasData = false

  }

  def text2Tuple(t: String): Tuple = {
    val textString = t.toString().split(",")
    var tuple = new java.util.ArrayList[String](textString.size)
    for (i <- 0 until textString.length) {
      tuple.add(textString(i))
    }
    Factories.tuple.newTupleNoCopy(tuple)
  }
}

class RandomWebsitesData() extends InputOperator {
  def copy = throw new Error("This shouldn't happen")
  private var _hasData = true
  def hasData = _hasData
  val entry1 = "text.com, 20000"
  val entry2 = "texta.com, 30000"
  val entry3 = "textb.com, 40000"
  val entry4 = "textc.com, 50000"
  val entry5 = "textd.com, 60000"

  def sendData(rt) = {
    rt.emit(0, text2Tuple(entry1))
    rt.emit(0, text2Tuple(entry2))
    rt.emit(0, text2Tuple(entry3))
    rt.emit(0, text2Tuple(entry4))

    _hasData = false

  }

  def text2Tuple(t: String): Tuple = {
    val textString = t.toString().split(",")
    var tuple = new java.util.ArrayList[String](textString.size)
    for (i <- 0 until textString.length) {
      tuple.add(textString(i))
    }
    Factories.tuple.newTupleNoCopy(tuple)
  }
}
