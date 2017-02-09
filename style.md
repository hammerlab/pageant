
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
  
## Omit Unnecessary Curly-Braces
In `if`/`else` statements, `match`/`case`s, `for`/`yield`s:
- Less syntactic clutter is better, other things being equal.
- In more idiomatic / less imperative Scala code, curly-braces are frequently unnecessary as blocks consist of single/pure expressions. 
  - Against this backdrop, the presence of curly-braces is often a good warning-flag to readers of the code that something suspicious or side-effect-ful may be occurring…

It is not unnoticed that this goes against much conventional wisdom around not relying on the "single-expression blocks can shed enclosing curlies" functionality offered by most major languages.
  
  
