(vale)=

# How to use Vale

# What is Vale?

[Vale](https://vale.sh/) is a tool that enforces the 
[Google developer documentation style guide](https://developers.google.com/style) on the
Ray documentation. 

Vale catches typos and grammatical errors. It also enforces stylistic rules like 
“use contractions” and “use second person.” For the full list of rules, see the 
[configuration in the Ray repository](https://github.com/ray-project/ray/tree/master/.vale/styles/Google).

# How do you run Vale?

- **To use the VSCode extension complete the following steps:**
    1. Install Vale. If you don’t use MacOS, see the 
    [Vale documentation](https://vale.sh/docs/vale-cli/installation/) for instructions.
        
        ```bash
        brew install vale 
        ```
        
    2. Install the Vale VSCode extension by following these 
    [installation instructions](https://marketplace.visualstudio.com/items?itemName=ChrisChinchilla.vale-vscode).

    3. VSCode should show warnings in your code editor and in the “Problems” panel.
        
        ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/54651db3-850c-4788-a420-fb6420c14f7f/Untitled.png)
        
- **To run Vale on the command-line, complete the following steps:***
    1. Install Vale. If you don’t use MacOS, see the 
    [Vale documentation](https://vale.sh/docs/vale-cli/installation/) for instructions.
        
        ```bash
        brew install vale 
        ```
        
    2. Run Vale in your terminal
        
        ```bash
        vale doc/source/data/overview.rst
        ```
        
    3. Vale should show warnings in your terminal.
        
        ```
        ❯ vale doc/source/data/overview.rst 
        
         doc/source/data/overview.rst
         18:1   warning     Try to avoid using              Google.We           
                            first-person plural like 'We'.                      
         18:46  error       Did you really mean             Vale.Spelling       
                            'distrbuted'?                                       
         24:10  suggestion  In general, use active voice    Google.Passive      
                            instead of passive voice ('is                       
                            built').                                            
         28:14  warning     Use 'doesn't' instead of 'does  Google.Contractions 
                            not'.                                               
        
        ✖ 1 error, 2 warnings and 1 suggestion in 1 file.
        ```
        

# Vale doesn’t recognize a word. What do you do?

To add custom terminology, complete the following steps:

1. If it doesn’t already exist, create a directory for your team in 
`.vale/styles/Vocab`. For example, `.vale/styles/Vocab/Data`.
2. If it doesn’t already exist, create a text file named `accept.txt`. For example, 
`.vale/styles/Vocab/Data/accept.txt`.
3. Add your term to `accept.txt`. Vale accepts Regex.

For more information, see [Vocabularies](https://vale.sh/docs/topics/vocab/) in the Vale 
documentation.