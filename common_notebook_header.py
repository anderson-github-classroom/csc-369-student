display_available = True
try:
    display('Verifying you can use display')
    from IPython.display import Image
except:
    display_available = False
    display = print
    
try:
    import pygraphviz
    graphviz_installed = True # Set this to False if you don't have graphviz
except:
    graphviz_installed = False