package optics

trait Status
case object Normal extends Status
case object Anomaly extends Status
