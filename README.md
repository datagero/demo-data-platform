# Practical Data Platform From Scratch

## Overview

**Practical Data Platform** is a project designed to build a generic data platform from the ground up, while also serving as an educational resource for aspiring data and platform engineers. Inspired by a recent research project requiring robust data processing, cataloging, and machine learning capabilities, this platform aims to tackle real-world data challenges with a best-in-class approach, even when starting from scratch. The project documents the entire development process, including key decisions, tool evaluations, and architectural changes, to provide a practical learning experience.

## Inspiration

The inspiration for this project comes from a recent research initiative involving various data files that need careful processing to build machine learning models and drive meaningful analysis. As part of this project, we face the challenge of managing diverse datasets while maintaining high standards of data quality. With multiple stakeholders involved, there is also a need to effectively communicate the quality of data and potentially recommend better methods for data collection and processing.

Recognizing the importance of a structured approach, I saw no reason why such a project should not leverage a best-in-class data platform. Moreover, as I anticipate being involved in similar projects in the future, I wanted to create a generic platform that would allow me to build robust processes for these tasks. 

## Purpose

The primary purpose of this project is to construct a flexible and scalable data platform using open-source tools. Initially, the platform is designed to operate without relying on cloud services, as I do not currently have a budget for such infrastructure. However, the platform's architecture is intentionally designed to be adaptable, allowing for future integrations with multi-cloud services if needed.

Throughout the development process, I quickly realized there were numerous decisions to make regarding frameworks, tools, and overall architecture. Although I have a general vision for where I want the project to end up, I discovered that building a data platform from scratch involves iterative changes, including refactoring and migrating between different tools and frameworks. My personal notes on these decisions became extensive, and I recognized the value in documenting this process to provide insights and learning opportunities for aspiring data and platform engineers.

## Approach to Documentation

To achieve the goal of making this project educational, I have chosen to document every significant step through a series of pull requests (PRs) that serve as both the platform setup and initial tooling implementation. Each PR is accompanied by a small blog-style commentary that explains the changes made, discusses the business and engineering impact, and outlines the thought process behind each decision. Readers can review the `git diff` inside the PR for a deeper understanding of the changes, and additional issues attached to the PRs delve further into specific questions and decision-making considerations.

This documentation approach provides a living guide for others, allowing them to learn on the go and understand the real-world implications of the choices made in building a data platform.

## 📘 How to Use This Repo

This repository is structured to guide you through the journey of building a data platform from scratch:

- **Explore the Pull Requests (PRs):** Each PR serves as a mini-blog post detailing the changes made, the rationale behind them, and their business and engineering impact. The PRs also include a `git diff` to give you a deeper understanding of the modifications.
- **Dive into the Issues:** Each issue attached to the PRs provides background information, research into relevant tools, and insights into the decision-making process.
- **Understand the Architecture:** The project encompasses a range of items typical in data platform architecture, including data ingestion, schema validation, data profiling, pipeline orchestration, monitoring, and logging. 

## Invitation to Collaborate

I also welcome other engineers to contribute their questions, suggestions, and discussion points. While the project aims to establish a custom framework that ties the platform together, the vision is to create a modular, plug-and-play environment where different technologies and methodologies can be tested and evaluated. This approach allows the platform to function as a sandbox for experimenting with various tools, fostering collaboration, and driving innovation in data engineering.
