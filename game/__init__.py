import os
import sys
print(os.path.abspath(__file__))
print(os.path.dirname(os.path.abspath(__file__)))
print(os.path.join(os.path.dirname(os.path.abspath(__file__))))
print(sys.path)