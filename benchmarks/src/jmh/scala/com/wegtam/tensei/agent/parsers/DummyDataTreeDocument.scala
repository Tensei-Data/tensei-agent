package com.wegtam.tensei.agent.parsers

import akka.actor.{Actor, ActorLogging, Props}

class DummyDataTreeDocument extends Actor with ActorLogging {

  override def receive: Receive = {
    case _ => // We ignore all messages...
  }

}

object DummyDataTreeDocument {

  def props: Props = Props(classOf[DummyDataTreeDocument])

}
