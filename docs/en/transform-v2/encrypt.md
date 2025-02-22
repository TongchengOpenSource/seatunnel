# Replace

> Replace transform plugin

## Description

Examines string value in a given field and replaces substring of the string value that matches the given string literal or regexes with the given replacement.

## Options

|     name      |  type   | required | default value |
|---------------|---------|----------|---------------|
| encrypt_field | string  | yes      |               |
| encrypt_name  | string  | no       | MD5           |

### encrypt_field [string]

The field you want to encrypt

### encrypt_name [string]

The old string that will be replaced

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Example

The data read from source is a table like this:

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

We want to replace the char ` ` to `_` at the `name` field. Then we can add a `Replace` Transform like this:

```
transform {
  Replace {
    plugin_input = "fake"
    plugin_output = "fake1"
    replace_field = "name"
    pattern = " "
    replacement = "_"
    is_regex = true
  }
}
```

Then the data in result table `fake1` will update to

|   name   | age | card |
|----------|-----|------|
| Joy_Ding | 20  | 123  |
| May_Ding | 20  | 123  |
| Kin_Dom  | 20  | 123  |
| Joy_Dom  | 20  | 123  |

## Job Config Example

```
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
      }
    }
  }
}

transform {
  Replace {
    plugin_input = "fake"
    plugin_output = "fake1"
    replace_field = "name"
    pattern = ".+"
    replacement = "b"
    is_regex = true
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

## Changelog

### new version

- Add Replace Transform Connector

