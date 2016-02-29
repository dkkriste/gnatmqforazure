# GnatMQ for Azure

![](images/gnat.jpg)

GnatMQ for Azure - MQTT Broker for Azure

*Project Description*

Project to develop a high performance broker (server) for the MQTT protocol, an M2M Internet-of-Things communication protocol based on .Net Framework to run on Azure Cloud Service (PaaS). 

MQTT, short for Message Queue Telemetry Transport, is a light weight messaging protocol that enables embedded devices with limited resources to perform asynchronous communication on a constrained network.

Developed by IBM and Eurotech, the MQTT protocol is released as an open standard and being standardized by OASIS (Organization for the Advancement of Structured Information Standard), a non-profit consortium that drives the development, convergence and adoption of open standards for the global information society.

In general, the MQTT environment consists of multiple clients and a server, called broker.

This project is created to develop an MQTT broker.  While there are other MQTT broker project, this project is created to provide additional resources to learn and engage in developing useful resources for the MQTT protocol, in conjunction with the M2Mqtt project, a .NET client library for MQTT that supports .NET Framework, .NET Compact Framework and .NET Micro Framework.

The project has an official website here :  http://www.m2mqtt.net

For more information about MQTT, visit: http://www.mqtt.org

For more information about OASIS, visit: https://www.oasis-open.org

There is an MQTT client, M2Mqtt released as community resource on this GitHub repo : https://github.com/ppatierno/m2mqtt
This project is based on the GnatMQ released here: https://github.com/ppatierno/m2mqtt

*Main features included in the current release :*

* All three Quality of Service (QoS) Levels (at most once, at least once, exactly once);
* Clean session;
* Retained messages;
* Will message (QoS, topic and message);
* Username/Password via a User Access Control;
* Subscription to topics with wildcards; 
* Publish and subscribe handle using inflight queue;

Features not included in the current release :

* Broker configuration using a simple config file;
* Bridge configuration (broker to broker);
* Sessions, retained and will messages persisted at broker shutdown (ex. database); 
