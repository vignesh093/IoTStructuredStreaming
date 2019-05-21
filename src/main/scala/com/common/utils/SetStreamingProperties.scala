package com.common.utils

import java.util.ResourceBundle
import java.util.Properties


object SetStreamingProperties {
   val prop = new Properties()
        val rb = ResourceBundle.getBundle("streamingconf")
        val zkQuorum = rb.getString("zkQuorum").trim()
        val zkport = rb.getString("zkport").trim()
        val metadatatablename = rb.getString("metadatatablename").trim()
        val tstablename = rb.getString("tstablename").trim()
        val errortablename = rb.getString("errortablename").trim()
        val aggtimewindow = rb.getString("aggtimewindow").trim()
        val watermarkwindow = rb.getString("watermarkwindow").trim()
        val kafkabootstrap = rb.getString("kafkabootstrap").trim()
        val inputtopic = rb.getString("inputtopic").trim()
        val outputtopicwindow = rb.getString("outputtopicwindow").trim()
        val outputtopicevent = rb.getString("outputtopicevent").trim()
        
}