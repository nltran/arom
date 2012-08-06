// (c) AROM processing 

/*
 * This is a simple AROM hello world example. We first define an 
 * InputOperator which job is to emit words contained in an array to 
 * the PrintLN operator bundled with the arom-core package. 
 */

package org.arom.examples
import org.arom.core.InputOperator
import org.arom.core.PrintLN
import org.arom.core.LocalJob

class WordGenerator extends InputOperator {

    val words = Array("Hello", "World", "!")

    /*
   * This input operator is unique and should not be copied , i.e. used in conjunction with the ^ connector
   */
    override def copy = throw new Error("This shouldn't happen since this operator is unique")
    
    /*
   * As this is an InputOperator, it is not supposed to receive data. 
   * The core process of the operator is in the sendData method
   */
    private var _hasData = true
    def hasData = _hasData

    override def sendData(rt) = {
      words foreach (f => rt.emit(0, f))
      _hasData = false
    }
  }

object HelloWorldJob {
   
  def main(args: Array[String]) = {
    /*
     * Create a new job DAG using the connector notation
     * PrintLN is an operator bundled within the arom-core package
     */
    val helloWorldPlan = (new WordGenerator >> new PrintLN)
    /*
     * A LocalJob is created which will run locally on a single machine
     * This is useful for local testing before deploying on a cluster
     * On launch, a swing job viewer will show the DAG corresponding to the job
     */
    val job = new LocalJob(helloWorldPlan)
    job.start
    
  }

}