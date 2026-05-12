import re

file_path = "services/signal_engine/engine.py"
with open(file_path, "r") as f:
    text = f.read()

# 1. Update import
text = text.replace("from ai.ml.signal_classifier import SignalClassifier", "from services.ml_worker.client import MLWorkerClient")

# 2. Update initialization
text = text.replace("self.ml_classifier = SignalClassifier() if settings.ml_validator_enabled else None", "self.ml_classifier = MLWorkerClient() if settings.ml_validator_enabled else None")

# 3. Update predict_proba line
text = text.replace("ml_prob = self.ml_classifier.predict_proba(ml_features)", "ml_prob = await self.ml_classifier.predict_proba(ml_features)")

# 4. We also need to start and stop the MLWorkerClient
start_sig = "async def start(self):"
start_add = """    async def start(self):
        if self.ml_classifier:
            await self.ml_classifier.start()
"""
text = text.replace(start_sig, start_add)

stop_sig = "async def stop(self):"
stop_add = """    async def stop(self):
        if self.ml_classifier:
            await self.ml_classifier.stop()
"""
text = text.replace(stop_sig, stop_add)

# 5. Remove is_ready check because client handles it or returns 0.5
text = text.replace("if self.ml_classifier and self.ml_classifier.is_ready():", "if self.ml_classifier:")

with open(file_path, "w") as f:
    f.write(text)
    
print("Patch applied")

