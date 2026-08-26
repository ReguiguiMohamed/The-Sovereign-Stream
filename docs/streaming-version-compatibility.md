# EP-010: Java, Flink and Flink Kubernetes Operator compatibility

Access date for every source below: **25 August 2026**. Only official Apache
Flink, Apache Flink Kubernetes Operator, Eclipse Adoptium and Google Kubernetes
Engine documentation was used. No blog post, search summary or third-party
compatibility claim is cited.

"Documented compatibility" is what the source states. "Selected" is an
EventProof project decision and carries no upstream guarantee.

## Headline finding

The newest stable Flink release and the newest stable Operator release are
**not** compatible with each other. Flink 2.3.0 is the latest stable Flink, but
Operator 1.15.0 documents support only up to Flink 2.2.x. EventProof therefore
selects Flink 2.2.1, not 2.3.0.

A second trap: the Operator's `flink-kubernetes-operator-docs-stable` URL alias
currently serves **v1.16.0** documentation, but no 1.16.0 release exists on the
Apache downloads page and no 1.16.0 release announcement was found. All Operator
evidence below is taken from the `release-1.15` documentation and the 1.15.0
release announcement instead.

## Candidates and documented compatibility

| Component | Candidate stable version | Documented compatibility | Java runtime requirement | Kubernetes requirement | Source |
| --- | --- | --- | --- | --- | --- |
| Apache Flink | 2.3.0 (released 2026-06-25) | Not listed as supported by Operator 1.15.0 | Java 17 default and recommended; Java 11 supported; Java 21 experimental | n/a for MiniCluster | [downloads](https://flink.apache.org/downloads/) |
| Apache Flink | 2.2.1 (released 2026-05-15) | Listed as supported by Operator 1.15.0 (`2.2.x`) | Java 17 default and recommended; Java 11 supported since 1.10.0; Java 21 experimental since 2.0.0 | n/a for MiniCluster | [java compatibility](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/java_compatibility/) |
| Apache Flink | 1.20.5 LTS (released 2026-06-03) | Listed as supported by Operator 1.15.0 (`1.20.x`) | Flink 1.x line; not evaluated further | n/a for MiniCluster | [downloads](https://flink.apache.org/downloads/) |
| Flink Kubernetes Operator | 1.15.0 | "2.2.x, 2.1.x, 2.0.x, 1.20.x, 1.19.x"; operator docs state "Multiple Flink version support: v1.19, v1.20, v2.0, v2.1, v2.2" | Not stated in the release announcement | Not stated as a support matrix; quickstart uses `minikube start --kubernetes-version=v1.28.0` and notes "end-to-end tests are using the same version" | [1.15.0 announcement](https://flink.apache.org/2026/05/26/apache-flink-kubernetes-operator-1.15.0-release-announcement/), [1.15 overview](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/docs/concepts/overview/), [1.15 quickstart](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/docs/try-flink-kubernetes-operator/quick-start/) |
| Flink Kubernetes Operator | 1.16.0 | No release on the downloads page and no release announcement found; docs alias only | Unknown | Unknown | [downloads](https://flink.apache.org/downloads/) |
| Java runtime | Temurin JDK 17 (LTS) | Flink 2.x: "We use Java 17 by default in Flink 2.0.0 and is the recommended Java version to run Flink on." Adoptium support "At least Oct 2027" | — | — | [java compatibility](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/java_compatibility/), [Adoptium support](https://adoptium.net/support/) |
| Java runtime | Temurin JDK 21 (LTS) | Flink 2.x: "Experimental support for Java 21 was added in 2.0.0." Adoptium support "At least Dec 2029" | — | — | [java compatibility](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/java_compatibility/), [Adoptium support](https://adoptium.net/support/) |
| Build tool | Apache Maven | Flink 2.2 project configuration states the requirements "Maven 3.8.6" and "Java 11" | Build requires Java 11 or later | — | [maven configuration](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/configuration/maven/) |
| MiniCluster test dependency | `org.apache.flink:flink-test-utils:2.2.1` (scope `test`) | "this module provides `MiniCluster`, a lightweight configurable Flink cluster runnable in a JUnit test that can directly execute jobs" | — | — | [testing configuration](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/configuration/testing/) |
| Kubernetes (GKE) | 1.34, 1.35, 1.36 | GKE standard support ends 2027-01-25, 2027-04-11 and 2027-08-09. "GKE version 1.29 and earlier have reached end of support and are no longer supported." Versions 1.30–1.33 have standard-support end dates on or before 2026-08-12, i.e. already reached at this access date | — | — | [GKE release schedule](https://docs.cloud.google.com/kubernetes-engine/docs/release-schedule) |

## Selection

| Component | Selected version | Reason for selection |
| --- | --- | --- |
| Apache Flink | **2.2.1** | Highest Flink release that Operator 1.15.0 documents as supported. Flink 2.3.0 is newer and stable but falls outside the Operator's documented support list, so choosing it would break the Phase 2 path to GKE. |
| Flink Kubernetes Operator | **1.15.0** | The only version the Apache downloads page calls the latest stable release. Operator 1.16.0 documentation exists at the `docs-stable` alias but has no corresponding release, so it is not selectable. |
| Java runtime | **Temurin JDK 17 (LTS)** | Flink's default and recommended runtime for the 2.x line, and it satisfies the Maven build requirement of Java 11 or later. Java 21 is only experimental for Flink 2.x; Java 11 is supported but is not the recommended runtime. Adoptium support runs to at least Oct 2027. |
| Build tool | **Apache Maven 3.8.6 or later** | The version Flink's own project-configuration page states as the requirement, and the form in which Flink documents the MiniCluster test dependency. |
| Test dependency | **`org.apache.flink:flink-test-utils:2.2.1`, scope `test`** | The single artifact Flink documents as providing `MiniCluster`. It is the smallest dependency that supports EP-012 and EP-013. |
| Kubernetes | **Not pinned in EP-010** | Deferred to EP-020. EP-011 through EP-013 run in a local MiniCluster and need no Kubernetes cluster. When Kubernetes is pinned, only GKE 1.34–1.36 remain within standard support at this access date. |

The selection was executed locally on 25 August 2026 with portable Temurin JDK
17.0.20.1 and Maven 3.9.16. `mvn --file streaming/pom.xml verify` compiled the
Flink 2.2.1 job and passed three tests, including two MiniCluster scenarios.

## Remaining uncertainty

- The Operator publishes no Kubernetes support matrix. The only Kubernetes
  version in its 1.15 documentation is the quickstart's `v1.28.0`, which is
  below every GKE version still in standard support. Whether Operator 1.15.0 is
  validated on GKE 1.34–1.36 is not documented by Apache and must be treated as
  untested until EP-020.
- The Operator 1.15.0 release announcement is dated 26 May 2026 while the
  downloads page lists a 1.15.0 date of 24 July 2026. The version selection does
  not depend on which date is correct, but the discrepancy is unresolved.
- The Operator 1.16.0 documentation at the `docs-stable` alias may indicate an
  in-progress release. If 1.16.0 ships and documents Flink 2.3.x, the Flink
  selection should be revisited before EP-011 hardens.
- Flink's Maven page states a Java 11 requirement while its Java compatibility
  page recommends Java 17 for the 2.x line. Selecting Java 17 satisfies both,
  but Apache does not state a single Java version for build and runtime
  together.
- EventProof uses the JUnit 4 `MiniClusterWithClientResource` documented by Flink
  2.2.1. The released artifact places `MiniClusterResourceConfiguration` in
  `org.apache.flink.runtime.testutils`, not the package shown by the documentation
  example; the artifact package is used.
- The current POJO state path passes under JDK 17 without explicit
  `--add-opens` or `--add-exports`. This does not prove that later connectors or
  serializers will need no module flags.
- These results prove a local build and MiniCluster behavior only. They are not
  evidence of Operator, Kubernetes or GCP behavior.
