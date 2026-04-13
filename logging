```python
logging.basicConfig(level=logging.INFO)
```

---

## What is logging? 🤔

Logging means:

> **Keeping track of what your program is doing**

Instead of using:

```python
print("Download started")
```

We use:

```python
logging.info("Download started")
```

---

## Why not just use `print()`?

Because logging is:

* More professional ✅
* Can be controlled (show/hide messages) ✅
* Can be saved to file ✅
* Used in real companies ✅

---

## What does `basicConfig()` do?

```python
logging.basicConfig(level=logging.INFO)
```

This line tells Python:

> “Start logging, and show messages of level INFO and above”

---

## What is `level=logging.INFO`?

Logging has levels (like priority):

| Level    | Meaning                       |
| -------- | ----------------------------- |
| DEBUG    | Detailed info (developer use) |
| INFO     | Normal updates (your case)    |
| WARNING  | Something unusual             |
| ERROR    | Something failed              |
| CRITICAL | Big failure                   |

---

### Your setting:

```python
level=logging.INFO
```

👉 Means show:

* INFO ✅
* WARNING ✅
* ERROR ✅
* CRITICAL ✅

👉 Hide:

* DEBUG ❌

---

## Example from your code

```python
logging.info("Downloading file...")
```

Because level = INFO → it will print:

```
INFO:root:Downloading file...
```

---

## What if level was WARNING?

```python
logging.basicConfig(level=logging.WARNING)
```

Then:

* `logging.info()` ❌ won’t show
* `logging.error()` ✅ will show

---

## Simple analogy 🧠

Think of logging like WhatsApp notifications:

* DEBUG → every small message
* INFO → normal updates
* WARNING → something odd
* ERROR → problem
* CRITICAL → emergency

You chose:

> “Show me normal updates and above”

---

## Why this is useful in your script

Your script:

* Downloads files
* Uploads files
* Connects to servers

Logging helps you see:

* What is happening
* Where it failed
* Which file caused error

---

## One-line explanation (interview ready 🎯)

> “logging.basicConfig(level=logging.INFO) configures logging to display informational messages and above during program execution.”

---

## 🐼 Simple memory line

> Logging = smarter print()
