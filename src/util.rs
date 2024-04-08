use std::path::{Component, Path, PathBuf};

// https://github.com/rust-lang/cargo/blob/fede83ccf973457de319ba6fa0e36ead454d2e20/src/cargo/util/paths.rs#L61
pub fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_normalize_path() -> Result<()> {
        assert_eq!(
            normalize_path(Path::new("/hello/world")),
            PathBuf::from("/hello/world")
        );

        assert_eq!(
            normalize_path(Path::new("/hello/world/../foo")),
            PathBuf::from("/hello/foo")
        );

        assert_eq!(
            normalize_path(Path::new("/../hello/world")),
            PathBuf::from("/hello/world")
        );

        assert_eq!(
            normalize_path(Path::new("../hello/world")),
            PathBuf::from("hello/world")
        );

        assert_eq!(
            normalize_path(Path::new("../hello/world/../foo/bar")),
            PathBuf::from("hello/foo/bar")
        );

        Ok(())
    }
}
