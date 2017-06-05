
# Style Guide

This is a living doc with discussion and assertions of some stylistic preferences found in this repo's modules.

## Duplicating package names in file names
Filenames should not have the last segment of the package-name prepended to them:

Less ideal:

```
org.hammerlab.apples.ApplesFoo
org.hammerlab.oranges.OrangesFoo
```

More ideal:

```scala
org.hammerlab.apples.Foo
org.hammerlab.oranges.Foo
```

### Discussion
Inside `org.hammerlab.genomics.apples`, `Foo` should refer to the package-local "Foo" abstraction by default, if one exists. 

If there is ambiguity at a specific import-site about which of multiple packages' `Foo`s is being referenced, that can be resolved by either:

- `import`-renaming:

  ```scala
  import org.hammerlab.genomics.apples.{ Foo => ApplesFoo }
  import org.hammerlab.genomics.oranges.{ Foo => OrangesFoo }
  …
  def applesToOranges(fromFoo: ApplesFoo, toFoo: OrangesFoo) = { … }
  ```

- package-namespacing references:

  ```scala
  import org.hammerlab.apples
  import org.hammerlab.oranges
  …
  def applesToOranges(fromFoo: apples.Foo, toFoo: oranges.Foo) = { … }
  ```

## Import symbols as specifically as possible

Related to the above, prefer:

```scala
import java.nio.file.Files.newByteChannel

…
val channel = newByteChannel(…)
```

over:

```scala
import java.nio.file.Files

…
val channel = Files.newByteChannel(…)
```

`Files`-scoping of `newByteChannel` is only necessary if there are multiple `newByteChannel` methods/symbols in scope / that could potentially be referred to.
  
A common corollary is that `math.{ min, max, ceil, floor, … }` should all be imported and used by name directly, as opposed to being referenced throughout code as `math.min(…)`, `math.max(…)`, etc.; we know what `min` and `max` mean without specifying what top-level-subpackage they are declared in, and more importantly don't care about the latter when we are expressing logic.

## Omit Unnecessary Curly-Braces
In `if`/`else` statements, `match`/`case`s, `for`/`yield`s, method-definitions, etc.:
- Less syntactic clutter is better, other things being equal.
- In more idiomatic / less imperative Scala code, curly-braces are frequently unnecessary as blocks consist of single/pure expressions. 
  - Against this backdrop, the presence of curly-braces is often a good warning-flag to readers of the code that something suspicious or side-effect-ful may be occurring…

It is not unnoticed that this goes against much conventional wisdom around not relying on the "single-expression blocks can shed enclosing curlies" functionality offered by most major languages.
  
## Type-aliases / Value-types

To maximally leverage Scala's type-sophistication, push domain-logic into the type-system.
 
For example, replace "bags of primitives" with [value-types](http://docs.scala-lang.org/overviews/core/value-classes.html) or other records:
 
### Before

```scala
def loadRecordsFromSample(
    filePath: String, 
    sampleName: String, 
    sampleId: Int
): Array[(Int, String)]
```

### After

```scala
import java.nio.file.Path

// Sample.scala or sample package object
class SampleName(name: String) extends AnyVal
class SampleId(id: Int) extends AnyVal

// SampleRecord.scala or record package object
class RecordId(id: Int) extends ANyVal
class RecordValue(value: String) extends AnyVal
case class SampleRecord(id: RecordId, value: RecordValue)

def loadRecordsFromSample(
    path: Path, 
    sampleName: SampleName, 
    sampleId: SampleId
): Array[SampleRecord]
```

### Redundant Variable Names

As a corollary, a desirable property is that variable-names start to become redundant with the types they represent (cf. `path: Path`, `sampleName: SampleName`, `sampleId: SampleId` above).
