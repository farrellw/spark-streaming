package com.farrellw.models

case class SlackMessage(text: String, mood: Option[String], timestamp: String, innovationHours: Boolean)