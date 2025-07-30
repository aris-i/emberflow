function view(entity: string, props: string[], options?: Record<string, any>): string {
  const optionStr = Object.entries(options || {})
    .map(([key, value]) => `${key}=${value}`)
    .join(",");

  return `View:${entity}:${props.join(",")}:${optionStr}`;
}

export {view};
