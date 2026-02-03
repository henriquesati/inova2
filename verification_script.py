import sys
import os
sys.path.append(os.getcwd())

try:
    from clientside.transaction.empenho_transaction import EmpenhoTransaction
    from models.empenho import Empenho
    
    # Check if empenhos field exists
    import dataclasses
    fields = [f.name for f in dataclasses.fields(EmpenhoTransaction)]
    if 'empenhos' not in fields:
        print("ERROR: EmpenhoTransaction missing 'empenhos' field")
        sys.exit(1)
        
    print("EmpenhoTransaction refactoring verified.")
except Exception as e:
    print(f"Verification failed: {e}")
    sys.exit(1)
