# cio-flocker-driver
Enable load balancing, high availability and data mobility for your Docker containers http://www.convergeio.com


### Plugin for ClusterHQ/flocker
This is a plugin driver for the flocker project which enables Docker containers and data to move together between hosts in a cluster. Providing data persistence and high availability is critical for stateful services like databases running in containers. 

This driver combines with flocker to fully automate the movement of the container and data volume, making it easy to support stateful services using Docker containers. It also enables ConvergeIO's software to work as a storage backend for flocker. The virtual SAN approach of ConvergeIO's software eliminates physical data copying when a data volume is moved with its container. This capability enables the flocker orchestrated movement of container and data volume to happen  instantaneously. In addition, network I/O to copy data and CPU resources to manage the transfer are greatly reduced which limits any impact to the performance of containerized applications. 

### About Us
We're a small team of engineers with experience in enterprise storage systems. We see a need for open source software to deliver high performance, enterprise class, software defined storage for hyper-converged systems. 

This project is just beginning but under active development. Contributors are certainly welcome. If you have questions or feedback, please do let us know. We're looking forward to working with you on this project.

### Feature Requests
We're always looking for feature requests, suggestions and new problems to solve, so would definitely love to hear from you. 
