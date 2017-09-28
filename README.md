# GnatMQ for Azure

![](images/gnat.jpg)

GnatMQ for Azure - MQTT Broker for Azure

*Project Description*

Project to develop a high performance broker (server) for the MQTT protocol, based on .Net Framework to run on Azure Cloud Service (PaaS). 

Is is basically GnatMQ targeting ONLY .NET 4.5.2, and rewritten for high performance (32K+ concurrent connections and more than 10K messages pr second)

There is an MQTT client, M2Mqtt released as community resource on this GitHub repo : https://github.com/ppatierno/m2mqtt
This project is based on the GnatMQ released here: https://github.com/ppatierno/gnatmq

*Main features included in the current release :*

* All three Quality of Service (QoS) Levels (at most once, at least once, exactly once);
* Clean session;
* Retained messages;
* Will message (QoS, topic and message);
* Username/Password via a User Access Control;
* Publish and subscribe handle using inflight queue;

Features not included in the current release :

* Broker configuration using a simple config file;
* Bridge configuration (broker to broker);
* Sessions, retained and will messages persisted at broker shutdown (ex. database); 
