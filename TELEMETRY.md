# Enabling Telemetry

## Introduction

This documentation provides a guide on how to enable and use telemetry throughout the SDK. Monitoring activity empowers our clients to track and analyze behavior within the application. By integrating the telemetry functions provided, our clients gain valuable insights to identify potential issues, and make data-driven improvements to their software.

## Table of Contents

1. Configuration
2. Usage Example
3. MonitoredActivity Functions
4. Enabling Tracing


## 1. Configuration

Before using monitored activity functionality throughout the project, you'll need to configure an instance of the Azure Core Tracer interface and initialize a Kusto SDK Tracer with it. The Azure Core Tracer interface is part of the Azure Core Tracing library, which is a dependency of the Kusto SDK. The Kusto SDK Tracer is part of the Kusto SDK.

### OpenTelemetry Tracer Configuration

In the SampleApp project, you will also find Java code for enabling distributed tracing using the OpenTelemetry SDK. This code sets up a custom tracer for monitoring application spans. Customize the tracing configuration, as required.

## 2. Usage Example

The SampleApp project demonstrates how to use monitored activity telemetry along with the provided SDK functions. The main entry point of the application is the `SampleApp.main` method.

In the `SampleApp.runSampleApp` method, you will find the integration with the monitored activity SDK. The `MonitoredActivity.invoke` function is used to invoke the application's main logic while enabling monitored activity telemetry.

## 3. MonitoredActivity  Functions

The monitored activity package provides an invoker to help you track user activity:

- `MonitoredActivity.invoke`: Invokes the main logic of the application while enabling monitored activity telemetry.

Refer to the SDK documentation for more details on each function and their parameters.

## 4. Enabling Tracing

The SampleApp project also includes code for enabling distributed tracing using the OpenTelemetry SDK. This involves initializing and configuring the OpenTelemetry SDK with default values.

