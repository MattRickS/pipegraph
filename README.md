# Hierarchy
The pipeline is defined in three levels of hierarchy:
* Stage: Any entity which will have work assigned to it
* Workspace: A location for completing a specific work task
* Port: A point of connection between workspaces

A configuration file is used to define templates for how to construct and connect the workspaces and ports for stages. It is also possible to define conditional workspaces, ports, and connections which is driven by abstract metadata on the objects. When constructing an instance of a stage, data can be provided to drive which conditionals are evaluted. A custom expression syntax is used to access the data.

# Expression Syntax
The expression syntax is used to access values of objects to help determine conditional behaviour. Each use of an expression is provided with keywords that can be used as a starting point for accessing the data, and the available keywords vary depending on where the expression is defined.

The keywords "stage", "workspace", and "port" are defined for each step of the hierarchy the condition is beneath. For example, if the condition is beneath stage/workspace, "stage" and "workspace" are provided, but "port" is not.
"foreach" conditionals also provide a keyword "item" for the current iteration item.

Accessing data from the starting object can be done in one of two ways:
1. Dot syntax to access properties of the object in the same way as `getattr`. Example: `stage.attr`
2. The `getitem` syntax using `[]` operators. Note, when accessing metadata this will access the "value" key directly. Example: `stage[key]`
